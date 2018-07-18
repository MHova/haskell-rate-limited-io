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
  BackoffPolicy(..),
  RateManager,
) where


import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically, retry)
import Control.Concurrent.STM.TVar (TVar, newTVar, readTVar, writeTVar,
  modifyTVar)
import Control.Exception (finally)
import Control.Monad (join)
import Data.Default.Class (Default, def)
import Data.List (delete)


{- |
  Jobs to be executed must return a Result, indicating either successful
  completion or an operation that hit the rate limit.
-}
data Result a b = Ok a | HitLimit b

data BackoffPolicy = BackoffPolicy
  { initialDelayMilliseconds :: Int
  , maxExponent :: Int
  }
instance Default BackoffPolicy where
  def = BackoffPolicy
    { initialDelayMilliseconds = 10
    , maxExponent = 11
    }

-- | Calculate a new delay in microseconds. This is not a true exponential
--   backoff as the delays are not random.
--   https://en.wikipedia.org/wiki/Exponential_backoff
time ::
     Int
     -- ^ the initial delay in milliseconds
  -> Int
     -- ^ the number of collisions that have occurred
  -> Int
time initialDelayMs collisions =
    initialDelayMs * ((2 ^ collisions) - 1) * numMicrosecondsInMillisecond
  where
    numMicrosecondsInMillisecond :: Int
    numMicrosecondsInMillisecond = 1000

{- |
  We default the maximum backoff exponent to 11 and initial delay to 10ms, which
  translates to a 20.47 second delay. Specifying a higher maximum is useful for
  platforms that enforce a long waiting-period when a rate-limit is exceeded.

  backoff   time
  -------  -------
     0       0.0 (seconds)
     1      0.01
     2      0.03
     3      0.07
     4      0.15
     5      0.31
     6      0.63
     7      1.27
     8      2.55
     9      5.11
    10     10.23
    11     20.47
    12     40.95

    13      1.36 (minutes)
    14      2.73
    15      5.46
    16     10.92
    17     21.84
    18     43.69

    19      1.45 (hours)
-}


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

  The 'mkNextJob' argument is responsible for returning a tuple that
  both controls how the timeout is computed, and also specifies a new
  job to try. If the first element of the tuple is 'Nothing', then the
  job will be re-tried using an internally computed backoff
  timeout. If the first element of the tuple is 'Just Int', the 'Int'
  specifies explicitly the number of milliseconds to timeout before trying
  the new job.
-}
performWith ::
     BackoffPolicy
  -> RateManager
  -> (b -> (Maybe Int, IO (Result a b)))
  -> IO (Result a b)
  -> IO a
performWith policy R{countT, throttledT} mkNextJob job = do
  jobId <- freshJobId countT
  performJob policy throttledT jobId mkNextJob Nothing job


freshJobId :: TVar Int -> IO Int
freshJobId countT = atomically $ do
  c <- readTVar countT
  writeTVar countT (c + 1)
  return c

{- |
  The same as `performWith`, but the original job is retried each time.
-}
perform ::
     BackoffPolicy
  -> RateManager
  -> IO (Result a ())
  -> IO a
perform policy r job = performWith policy r (const (Nothing, job)) job

performJob ::
     BackoffPolicy
  -> TVar [Int]
  -> Int
  -> (b -> (Maybe Int, IO (Result a b)))
  -> Maybe Int
  -> IO (Result a b)
  -> IO a
performJob policy@BackoffPolicy{initialDelayMilliseconds, maxExponent}
  throttledT jobId mkNextJob timeout job =
    join . atomically $ do
      throttled <- readTVar throttledT
      case throttled of
        [] -> return tryJob -- full speed ahead.
        first:_ | first == jobId ->
          -- we are first in line
          return (untilSuccess 0 timeout job `finally` pop)
        _ ->
          -- we must wait
          retry
  where
    tryJob = do
      result <- job
      case result of
        Ok val -> return val
        HitLimit limitResponse -> do
          atomically $ modifyTVar throttledT (++ [jobId])
          uncurry (performJob policy throttledT jobId mkNextJob) $
            mkNextJob limitResponse

    untilSuccess collisions timeout' job' = do
      case timeout' of
        Nothing -> threadDelay (time initialDelayMilliseconds collisions)
        Just milliseconds -> threadDelay (1000 * milliseconds)
      result <- job'
      case result of
        Ok val -> return val
        HitLimit limitResponse ->
          uncurry (untilSuccess $ newExponent collisions)
            (mkNextJob limitResponse)

    newExponent collisions
      | collisions >= maxExponent = collisions
        -- ^ don't go crazy with the backoff exponent.
      | otherwise = collisions + 1

    pop = atomically $ modifyTVar throttledT (delete jobId)
