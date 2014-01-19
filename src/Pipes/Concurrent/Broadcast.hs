
module Pipes.Concurrent.Broadcast (
    Broadcast(..),
    broadcast,
    subscribe
    ) where

import Control.Applicative ((<|>), (<*), (<$>))
import Control.Concurrent.STM
    (STM, TVar, atomically, newTVarIO, writeTVar, readTVar, check)
import Control.Concurrent.STM.TChan
    (TChan, newBroadcastTChanIO, dupTChan, writeTChan, readTChan)
import Data.IORef (newIORef, readIORef, mkWeakIORef)
import Pipes.Concurrent (Input(..), Output(..))
import GHC.Conc.Sync (unsafeIOToSTM)

data Broadcast a
    = Broadcast
    { bcChan    :: TChan a
    , bcSealed  :: TVar Bool }

broadcast :: IO (Output a, Broadcast a, STM ())
broadcast = do
    ch <- newBroadcastTChanIO

    sealed <- newTVarIO False
    let seal = writeTVar sealed True

    rSend <- newIORef ()
    mkWeakIORef rSend (atomically seal)

    let sendOrEnd x = do
            b <- readTVar sealed
            if b
                then return False
                else do
                    writeTChan ch x
                    return True
        _send x = sendOrEnd x <* unsafeIOToSTM (readIORef rSend)

    return (Output _send, Broadcast { bcChan = ch, bcSealed = sealed }, seal)
{-# INLINABLE broadcast #-}

subscribe :: Broadcast a -> STM (Input a)
subscribe Broadcast {bcChan = ch, bcSealed = sealed} = do
    nch <- dupTChan ch

    let _recv = (Just <$> readTChan nch) <|> (do
            b <- readTVar sealed
            check b
            return Nothing )

    return (Input _recv)
{-# INLINABLE subscribe #-}

