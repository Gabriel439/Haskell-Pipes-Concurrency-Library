-- | Asynchronous communication between proxies

{-# LANGUAGE CPP #-}

#if __GLASGOW_HASKELL__ >= 702
{-# LANGUAGE Trustworthy #-}
#endif
{- 'unsafeIOToSTM' requires the Trustworthy annotation.

    I use 'unsafeIOToSTM' to touch an IORef to mark it as still alive. This
    action satisfies the necessary safety requirements because:

    * You can safely repeat it if the transaction rolls back

    * It does not acquire any resources

    * It does not leak any inconsistent view of memory to the outside world

    It appears to be unnecessary to read the IORef to keep it from being garbage
    collected, but I wanted to be absolutely certain since I cannot be sure that
    GHC won't optimize away the reference to the IORef.

    The other alternative was to make 'send' and 'recv' use the 'IO' monad
    instead of 'STM', but I felt that it was important to preserve the ability
    to combine them into larger transactions.
-}

module Control.Proxy.Concurrent (
    -- * Spawn mailboxes
    spawn,
    Buffer(..),
    Input,
    Output,

    -- * Send and receive messages
    send,
    recv,

    -- * Proxy utilities
    sendD,
    recvS,

    -- * Prefetching
    -- $prefetch
    prefetch,

    -- * Re-exports
    -- $reexport
    module Control.Concurrent,
    module Control.Concurrent.STM,
    module System.Mem
    ) where

import Control.Applicative ((<|>), (<*), pure)
import Control.Concurrent (forkIO)
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM (atomically, STM)
import Control.Monad.Trans.Class (lift)
import qualified Control.Concurrent.STM as S
import qualified Control.Proxy as P
import Control.Proxy ((>->))
import Data.IORef (newIORef, readIORef, mkWeakIORef)
import GHC.Conc.Sync (unsafeIOToSTM)
import System.Mem (performGC)

{-| Spawn a mailbox that has an 'Input' and 'Output' end, using the specified
    'Buffer' to store messages
-}
spawn :: Buffer a -> IO (Input a, Output a)
spawn buffer = do
    (read, write) <- case buffer of
        Bounded n -> do
            q <- S.newTBQueueIO n
            let read = do
                    ma <- S.readTBQueue q
                    case ma of
                        Nothing -> S.unGetTBQueue q ma
                        _       -> return ()
                    return ma
            return (read, S.writeTBQueue q)
        Unbounded -> do
            q <- S.newTQueueIO
            let read = do
                    ma <- S.readTQueue q
                    case ma of
                        Nothing -> S.unGetTQueue q ma
                        _       -> return ()
                    return ma
            return (read, S.writeTQueue q)
        Single    -> do
            m <- S.newEmptyTMVarIO
            let read = do
                    ma <- S.takeTMVar m
                    case ma of
                        Nothing -> S.putTMVar m ma
                        _       -> return ()
                    return ma
            return (read, S.putTMVar m)
        Latest a  -> do
            t <- S.newTVarIO a
            let write ma = case ma of
                    Nothing -> return ()
                    Just a  -> S.writeTVar t a
            return (fmap Just (S.readTVar t), write)

    {- Use an IORef to keep track of whether the 'Input' end has been garbage
       collected and run a finalizer when the collection occurs

       The finalizer cannot anticipate how many listeners there are, so it only
       writes a single 'Nothing' and trusts that the supplied 'read' action
       will not consume the 'Nothing'.

       The 'write' must be protected with the "pure ()" fallback so that it does
       not deadlock if the 'Output' end has also been garbage collected.
    -}
    rUp  <- newIORef ()
    mkWeakIORef rUp (S.atomically $ write Nothing <|> pure ())

    {- Use an IORef to keep track of whether the 'Output' end has been garbage
       collected and run a finalizer when the collection occurs
    -}
    rDn  <- newIORef ()
    done <- S.newTVarIO False
    mkWeakIORef rDn (S.atomically $ S.writeTVar done True)

    let quit = do
            b <- S.readTVar done
            S.check b
            return False
        continue a = do
            write (Just a)
            return True
        {- The '_send' action aborts if the 'Output' has been garbage collected,
           since there is no point wasting memory if nothing can empty the
           mailbox.  This protects against careless users not checking send's
           return value, especially if they use a mailbox of 'Unbounded' size.
        -}
        _send a = (quit <|> continue a) <* unsafeIOToSTM (readIORef rUp)
        _recv = read <* unsafeIOToSTM (readIORef rDn)
    return (Input _send , Output _recv)

{-| 'Buffer' specifies how to store messages sent to the 'Input' end until the
    'Output' receives them.
-}
data Buffer a
    -- | Store an 'Unbounded' number of messages in a FIFO queue
    = Unbounded
    -- | Store a 'Bounded' number of messages, specified by the 'Int' argument
    | Bounded Int
    -- | Store a 'Single' message (like @Bounded 1@, but more efficient)
    | Single
    {-| Store the 'Latest' message, beginning with a initial value

        'Latest' is never empty nor full.
    -}
    | Latest a

-- | Accepts messages for the mailbox
newtype Input a = Input {
    {-| Send a message to the mailbox

        * Fails and returns 'False' if the mailbox's 'Output' has been garbage
          collected (even if the mailbox is not full), otherwise it:

        * Retries if the mailbox is full, or:

        * Succeeds if the mailbox is not full and returns 'True'.
    -}
    send :: a -> S.STM Bool }

-- | Retrieves messages from the mailbox
newtype Output a = Output {
    {-| Receive a message from the mailbox

        * Succeeds and returns a 'Just' if the mailbox is not empty, otherwise
          it:

        * Retries if mailbox's 'Input' has not been garbage collected, or:

        * Fails if the mailbox's 'Input' has been garbage collected and returns
          'Nothing'.
    -}
    recv :: S.STM (Maybe a) }

{-| Writes all messages flowing \'@D@\'ownstream to the given 'Input'

    'sendD' terminates when the corresponding 'Output' is garbage collected.
-}
sendD :: (P.Proxy p) => Input a -> x -> p x a x a IO ()
sendD input = P.runIdentityK loop
  where
    loop x = do
        a <- P.request x
        alive <- lift $ S.atomically $ send input a
        if alive
            then do
                x2 <- P.respond a
                loop x2
            else return ()

{-| Convert an 'Output' to a 'P.Producer'

    'recvS' terminates when the 'Buffer' is empty and the 'Input' is garbage
    collected.
-}
recvS :: (P.Proxy p) => Output a -> () -> P.Producer p a IO ()
recvS output () = P.runIdentityP go
  where
    go = do
        ma <- lift $ S.atomically $ recv output
        case ma of
            Nothing -> return ()
            Just a  -> do
                P.respond a
                go

{- $prefetch
   Using 'prefetch' you can execute an entire 'P.Proxy' concurrently, which
   allows you to prefetch responses for downstream. For example, if you have
   a producer that does some IO, such as reading a file, and a downstream that
   also does IO such as database serialization, you could prefetch file reading
   so that the file can be read while database serialization occurs.
-}
prefetch
  :: (P.Proxy p, P.Proxy p') =>
     Int
     -- ^ The amount of 'P.response's to prefetch
     -> ((b' -> p' a' a b' b IO ()) -> IO a)
     -- ^ A way to execute the proxy - usually 'P.runProxy'
     -> (b' -> p' a' a b' b IO ())
     -- ^ A 'P.Proxy' to prefetch from
     -> () -> P.Producer p b IO ()
prefetch n run p () = P.runIdentityP $ do
    (i, o) <- lift (spawn (Bounded n))
    worker <- lift $ Async.async $ do
        run $ p >-> sendD i
        performGC
    lift $ Async.link worker
    recvS o ()
    lift (performGC >> Async.wait worker)

{- $reexport
    @Control.Concurrent@ re-exports 'forkIO', although I recommend using the
    @async@ library instead.

    @Control.Concurrent.STM@ re-exports 'atomically' and 'STM'.

    @System.Mem@ re-exports 'performGC'.
-}
