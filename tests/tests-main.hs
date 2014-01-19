module Main ( main ) where

import Control.Concurrent hiding (yield)
import Control.Concurrent.Async
import Control.Monad (forever)
import Pipes
import Pipes.Concurrent
import Pipes.Concurrent.Broadcast
import qualified Pipes.Prelude as P
import System.Exit
import System.IO
import System.Timeout

defaultTimeout :: Int
defaultTimeout = 100000         -- 0.1 s

labelPrint :: (Show a) => String -> Consumer a IO r
labelPrint label = forever $ do
  a <- await
  lift $ putStrLn $ label ++ ": " ++ show a

testSenderClose :: Buffer Int -> IO ()
testSenderClose buffer = do
    (output, input) <- spawn buffer
    t1 <- async $ do
        runEffect $ each [1..5] >-> toOutput output
        performGC
    t2 <- async $ do
        runEffect $   fromInput input
                  >-> P.chain (\_ -> threadDelay 1000)
                  >-> P.print
        performGC
    wait t1
    wait t2

testSenderCloseDelayedSend :: Buffer Int -> IO ()
testSenderCloseDelayedSend buffer = do
    (output, input) <- spawn buffer
    t1 <- async $ do
        runEffect $   each [1..5]
                  >-> P.tee (toOutput output)
                  >-> for cat (\_ -> lift $ threadDelay 2000)
        performGC
    t2 <- async $ do
        runEffect $   fromInput input
                  >-> P.chain (\_ -> threadDelay 1000)
                  >-> P.print
        performGC
    wait t1
    wait t2

testReceiverClose :: Buffer Int -> IO ()
testReceiverClose buffer = do
    (output, input) <- spawn buffer
    t1 <- async $ do
        runEffect $   each [1..]
                  >-> P.tee (toOutput output)
                  >-> P.chain (\_ -> threadDelay 1000)
                  >-> P.print
        performGC
    t2 <- async $ do
        runEffect $ for (fromInput input >-> P.take 10) discard
        performGC
    wait t1
    wait t2

testReceiverCloseDelayedReceive :: Buffer Int -> IO ()
testReceiverCloseDelayedReceive buffer = do
    (output, input) <- spawn buffer
    t1 <- async $ do
        runEffect $   each [1..]
                  >-> P.tee (toOutput output)
                  >-> P.chain (\_ -> threadDelay 1000)
                  >-> labelPrint "Send"
        performGC
    t2 <- async $ do
        runEffect $   fromInput input
                  >-> P.take 10
                  >-> P.chain (\_ -> threadDelay 800)
                  >-> labelPrint "Recv"
        performGC
    wait t1
    wait t2

testBroadcast :: IO ()
testBroadcast = do
    (feed, bc, _) <- broadcast
    eat1 <- atomically $ subscribe bc
    eat2 <- atomically $ subscribe bc
    t1 <- async $ do
        runEffect $   fromInput eat1
                  >-> P.take 10
                  >-> labelPrint "Arrived 1"
        performGC
    t2 <- async $ do
        runEffect $   fromInput eat2
                  >-> P.take 10
                  >-> labelPrint "Arrived 2"
        performGC
    runEffect $   each [1..10]
              >-> toOutput feed
    wait t1
    wait t2

runTest :: IO () -> String -> IO ()
runTest test name = do
    putStrLn $ "Starting test: " ++ name
    hFlush stdout
    result <- timeout defaultTimeout test
    case result of
        Nothing -> do putStrLn $ "Test " ++ name ++ " timed out. Aborting."
                      exitFailure
        Just _  -> do putStrLn $ "Test " ++ name ++ " finished."
    hFlush stdout

runTestExpectTimeout :: IO () -> String -> IO ()
runTestExpectTimeout test name = do
    putStrLn $ "Starting test: " ++ name
    hFlush stdout
    result <- timeout defaultTimeout test
    case result of
        Nothing -> putStrLn $ "Test " ++ name ++ " timed out as expected."
        Just _  -> do
            putStrLn $
                   "Test "
                ++ name
                ++ " finished, but a timeout was expected. Aborting."
            exitFailure
    hFlush stdout

main :: IO ()
main = do
    runTest (testSenderClose Unbounded) "UnboundedSenderClose"
    runTest (testSenderClose $ Bounded 3) "BoundedFilledSenderClose"
    runTest (testSenderClose $ Bounded 7) "BoundedNotFilledSenderClose"
    runTest (testSenderClose Single) "SingleSenderClose"
    runTestExpectTimeout (testSenderCloseDelayedSend $ Latest 42) "LatestSenderClose"
    --
    runTest (testReceiverClose Unbounded) "UnboundedReceiverClose"
    runTest (testReceiverClose $ Bounded 3) "BoundedFilledReceiverClose"
    runTest (testReceiverClose $ Bounded 7) "BoundedNotFilledReceiverClose"
    runTest (testReceiverClose Single) "SingleReceiverClose"
    runTest (testReceiverCloseDelayedReceive $ Latest 42) "LatestReceiverClose"
    --
    runTest (testBroadcast) "Broadcast"
