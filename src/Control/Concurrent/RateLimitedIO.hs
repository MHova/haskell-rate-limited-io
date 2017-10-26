{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{- |
  This module provides some basic logic to deal with rate limited IO actions
  that should be retried until successful. It provides an exponential backoff
  time, and it provides the ability to coordinate the rate limit over multiple
  threads of execution.
-}
module Control.Concurrent.RateLimitedIO (
  newRateManager,
  perform,
  performWith,
  Result(..),
  RateManager
) where


import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically, retry)
import Control.Concurrent.STM.TVar (TVar, newTVar, readTVar, writeTVar,
  modifyTVar)
import Control.Exception.Lifted (finally)
import Control.Monad (join)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl)
import Data.List (delete)


{- |
  Jobs to be executed must return a Result, indicating either successful
  completion or an operation that hit the rate limit.
-}
data Result a b = Ok a | HitLimit b


{- |
  A coordinating manager for rate limiting.
-}
data RateManager =
  R {
    countT :: TVar Int,
    throttledT :: TVar [Int]
  }


{- |
  Create a new RateManager.
-}
newRateManager :: IO RateManager
newRateManager = do
  countT <- atomically (newTVar minBound)
  throttledT <- atomically (newTVar [])
  return R {countT, throttledT}

{- |
  Perform a job in the context of the `RateManager`. The job blocks
  until the rate limit logic says it can go. If the job gets throttled,
  then it re-tries until it is successful (where "successful" means
  anything except `HitLimit`. Throwing an exception counts as "success"
  in this case).

  The job that's executed in each retry is created from the
  client-provided `mkNextJob` function, which accepts the result of the previous
  HitLimit as an input.

  The idea is that the oldest throttled job must complete before any other jobs
  (throttled or not) are allowed to start. Because of concurrency, "oldest" in
  this case means when we discovered the job was throttled, not when it was
  started.

  If there are no jobs that have been throttled, then it is a
  free-for-all. All jobs are executed immediately.
-}
performWith ::
     (MonadIO m, MonadBaseControl IO m)
  => RateManager
  -> (b -> m (Result a b))
  -> m (Result a b)
  -> m a
performWith R {countT, throttledT} mkNextJob job = do
  jobId <- liftIO . atomically $ do
    c <- readTVar countT
    writeTVar countT (c + 1)
    return c
  performJob throttledT jobId mkNextJob job

{- |
  The same as `performWith`, but the original job is retried each time.
-}
perform ::
     (MonadIO m, MonadBaseControl IO m)
  => RateManager
  -> m (Result a ())
  -> m a
perform r job = performWith r (const job) job

performJob ::
     (MonadIO m, MonadBaseControl IO m)
  => TVar [Int]
  -> Int
  -> (b -> m (Result a b))
  -> m (Result a b)
  -> m a
performJob throttledT jobId mkNextJob job =
  join . liftIO . atomically $ do
    throttled <- readTVar throttledT
    case throttled of
      [] -> return tryJob -- full speed ahead.
      first:_ | first == jobId ->
        -- we are first in line
        return (untilSuccess 0 job `finally` pop)
      _ ->
        -- we must wait
        retry
  where
    tryJob = do
      result <- job
      case result of
        Ok val -> return val
        HitLimit limitResponse -> do
          liftIO . atomically $ modifyTVar throttledT (++ [jobId])
          performJob throttledT jobId mkNextJob (mkNextJob limitResponse)

    untilSuccess backoff job' = do
      liftIO $ threadDelay (time backoff)
      result <- job'
      case result of
        Ok val -> return val
        HitLimit limitResponse -> untilSuccess (newBackoff backoff)
          (mkNextJob limitResponse)

    newBackoff backoff
      | backoff > 10 = backoff -- don't go crazy with the backoff.
      | otherwise = backoff + 1

    pop = liftIO . atomically $ modifyTVar throttledT (delete jobId)

    -- | the time in microseconds of the backoff value (which is an exponent)
    time :: Int -> Int
    time backoff = 10000 * ((2 ^ backoff) - 1)