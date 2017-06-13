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
  Result(..),
  RateManager
) where


import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically, retry)
import Control.Concurrent.STM.TVar (TVar, newTVar, readTVar, writeTVar,
  modifyTVar)
import Control.Exception (finally)
import Control.Monad (join)
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

  The idea is that the oldest throttled job must complete before any other jobs
  (throttled or not) are allowed to start. Because of concurrency, "oldest" in
  this case means when we discovered the job was throttled, not when it was
  started.

  If there are no jobs that have been throttled, then it is a
  free-for-all. All jobs are executed immediately.
-}
perform :: RateManager -> IO (Result a b) -> IO a
perform R {countT, throttledT} job = do
  jobId <- atomically $ do
    c <- readTVar countT
    writeTVar countT (c + 1)
    return c
  performJob throttledT jobId job


performJob :: TVar [Int] -> Int -> IO (Result a b) -> IO a
performJob throttledT jobId job =
  join . atomically $ do
    throttled <- readTVar throttledT
    case throttled of
      [] -> return tryJob -- full speed ahead.
      first:_ | first == jobId ->
        -- we are first in line
        return (untilSuccess 0 `finally` pop)
      _ ->
        -- we must wait
        retry
  where
    tryJob = do
      result <- job
      case result of
        Ok val -> return val
        HitLimit _ -> do
          atomically $ modifyTVar throttledT (++ [jobId])
          performJob throttledT jobId job

    untilSuccess backoff = do
      threadDelay (time backoff)
      result <- job
      case result of
        Ok val -> return val
        HitLimit _ -> untilSuccess (newBackoff backoff)

    newBackoff backoff
      | backoff > 10 = backoff -- don't go crazy with the backoff.
      | otherwise = backoff + 1

    pop = atomically $ modifyTVar throttledT (delete jobId)

    -- | the time in microseconds of the backoff value (which is an exponent)
    time :: Int -> Int
    time backoff = 10000 * ((2 ^ backoff) - 1)


