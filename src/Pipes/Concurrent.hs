-- | Asynchronous communication between pipes

{-# LANGUAGE RankNTypes, Safe #-}

module Pipes.Concurrent (
    -- * Inputs and Outputs
    Input(..),
    Output(..),

    -- * Pipe utilities
    fromInput,
    toOutput,

    -- * Actors
    spawn,
    withSpawn,
    Buffer(..),
    unbounded,
    bounded,
    latest,
    newest,

    -- * Re-exports
    -- $reexport
    module Control.Concurrent,
    module Control.Concurrent.STM,
    ) where

import Control.Applicative (
    Alternative(empty, (<|>)), Applicative(pure, (*>), (<*>)), (<*), (<$>) )
import Control.Concurrent (forkIO)
import Control.Concurrent.STM (atomically, STM)
import qualified Control.Concurrent.STM as S
import Control.Exception (bracket)
import Control.Monad (when,void, MonadPlus(..))
import Data.Monoid (Monoid(mempty, mappend))
import Pipes (MonadIO(liftIO), yield, await, Producer', Consumer')

{-| An exhaustible source of values

    'recv' returns 'Nothing' if the source is exhausted
-}
newtype Input a = Input {
    recv :: S.STM (Maybe a) }

instance Functor Input where
    fmap f m = Input (fmap (fmap f) (recv m))

instance Applicative Input where
    pure r    = Input (pure (pure r))
    mf <*> mx = Input ((<*>) <$> recv mf <*> recv mx)

instance Monad Input where
    return r = Input (return (return r))
    m >>= f  = Input $ do
        ma <- recv m
        case ma of
            Nothing -> return Nothing
            Just a  -> recv (f a)

instance Alternative Input where
    empty   = Input (return Nothing)
    x <|> y = Input $ do
        (i, ma) <- fmap ((,) y) (recv x) <|> fmap ((,) x)(recv y)
        case ma of
            Nothing -> recv i
            Just a  -> return (Just a)

instance MonadPlus Input where
    mzero = empty
    mplus = (<|>)

instance Monoid (Input a) where
    mempty = empty
    mappend = (<|>)

{-| An exhaustible sink of values

    'send' returns 'False' if the sink is exhausted
-}
newtype Output a = Output {
    send :: a -> S.STM Bool }

instance Monoid (Output a) where
    mempty  = Output (\_ -> return False)
    mappend i1 i2 = Output (\a -> (||) <$> send i1 a <*> send i2 a)

{-| Convert an 'Output' to a 'Pipes.Consumer'

    'toOutput' terminates when the 'Output' is exhausted.
-}
toOutput :: (MonadIO m) => Output a -> Consumer' a m ()
toOutput output = loop
  where
    loop = do
        a     <- await
        alive <- liftIO $ S.atomically $ send output a
        when alive loop
{-# INLINABLE toOutput #-}

{-| Convert an 'Input' to a 'Pipes.Producer'

    'fromInput' terminates when the 'Input' is exhausted.
-}
fromInput :: (MonadIO m) => Input a -> Producer' a m ()
fromInput input = loop
  where
    loop = do
        ma <- liftIO $ S.atomically $ recv input
        case ma of
            Nothing -> return ()
            Just a  -> do
                yield a
                loop
{-# INLINABLE fromInput #-}

{-| Spawn a mailbox using the specified 'Buffer' to store messages

> (output, input, seal) <- spawn buffer
> ...

    Using 'send' on the 'Output'

        * fails and returns 'False' if the mailbox is sealed, otherwise it:

        * retries if the mailbox is full, or:

        * adds a message to the mailbox and returns 'True'.

    Using 'recv' on the 'Input':

        * retrieves a message from the mailbox wrapped in 'Just' if the mailbox
          is not empty, otherwise it:

        * retries if the mailbox is not sealed, or:

        * fails and returns 'Nothing'.

    Use @seal@ to close the mailbox so that no more messages can be sent to it.
-}
spawn :: Buffer a -> IO (Output a, Input a, STM ())
spawn buffer = do
    (write, read) <- case buffer of
        Bounded n -> do
            q <- S.newTBQueueIO n
            return (S.writeTBQueue q, S.readTBQueue q)
        Unbounded -> do
            q <- S.newTQueueIO
            return (S.writeTQueue q, S.readTQueue q)
        Single    -> do
            m <- S.newEmptyTMVarIO
            return (S.putTMVar m, S.takeTMVar m)
        Latest a  -> do
            t <- S.newTVarIO a
            return (S.writeTVar t, S.readTVar t)
        New       -> do
            m <- S.newEmptyTMVarIO
            return (\x -> S.tryTakeTMVar m *> S.putTMVar m x, S.takeTMVar m)
        Newest n  -> do
            q <- S.newTBQueueIO n
            let write x = S.writeTBQueue q x <|> (S.tryReadTBQueue q *> write x)
            return (write, S.readTBQueue q)

    sealed <- S.newTVarIO False
    let seal = S.writeTVar sealed True
        sendOrEnd a = do
            b <- S.readTVar sealed
            if b
                then return False
                else do
                    write a
                    return True
        readOrEnd = (Just <$> read) <|> (do
            b <- S.readTVar sealed
            S.check b
            return Nothing )
    return (Output sendOrEnd, Input readOrEnd, seal)
{-# INLINABLE spawn #-}

{-| 'withSpawn' passes its enclosed action an 'Output' and 'Input' like you'd
    get from 'spawn', but automatically @seal@s them after the action completes.

> withSpawn buffer $ \(output, input) -> ...

    'withSpawn' is exception-safe, since it uses 'bracket' internally.
-}
withSpawn :: Buffer a -> ((Output a, Input a) -> IO r) -> IO r
withSpawn buffer action = bracket
    (spawn buffer)
    (\(_, _, seal) -> atomically seal)
    (\(output, input, _) -> action (output, input))

-- | 'Buffer' specifies how to buffer messages stored within the mailbox
data Buffer a
    = Unbounded
    | Bounded Int
    | Single
    | Latest a
    | Newest Int
    | New

{-# DEPRECATED Unbounded "Use `unbounded` instead" #-}
{-# DEPRECATED Bounded "Use `bounded` instead" #-}
{-# DEPRECATED Single "Use @`bounded` 1@ instead" #-}
{-# DEPRECATED Latest "Use `latest` instead" #-}
{-# DEPRECATED Newest "Use `newest` instead" #-}
{-# DEPRECATED New "Use @`newest` 1@ instead" #-}

-- | Store an unbounded number of messages in a FIFO queue
unbounded :: Buffer a
unbounded = Unbounded

-- | Store a bounded number of messages, specified by the 'Int' argument
bounded :: Int -> Buffer a
bounded 1 = Single
bounded n = Bounded n

{-| Only store the 'Latest' message, beginning with an initial value

    'Latest' is never empty nor full.
-}
latest :: a -> Buffer a
latest = Latest

{-| Like @Bounded@, but 'send' never fails (the buffer is never full).
    Instead, old elements are discard to make room for new elements
-}
newest :: Int -> Buffer a
newest 1 = New
newest n = Newest n

{- $reexport
    @Control.Concurrent@ re-exports 'forkIO', although I recommend using the
    @async@ library instead.

    @Control.Concurrent.STM@ re-exports 'atomically' and 'STM'.
-}
