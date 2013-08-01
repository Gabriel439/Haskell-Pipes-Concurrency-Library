{-| This module provides a tutorial for the @pipes-concurrency@ library.

    This tutorial assumes that you have read the @pipes@ tutorial in
    @Pipes.Tutorial@.

    I've condensed all the code examples into self-contained code listings in
    the Appendix section that you can use to follow along.
-}

module Pipes.Concurrent.Tutorial (
    -- * Introduction
    -- $intro

    -- * Work Stealing
    -- $steal

    -- * Termination
    -- $termination

    -- * Mailbox Sizes
    -- $mailbox

    -- * Broadcasts
    -- $broadcast

    -- * Updates
    -- $updates

    -- * Callbacks
    -- $callback

    -- * Safety
    -- $safety

    -- * Conclusion
    -- $conclusion

    -- * Appendix
    -- $appendix
    ) where

import Pipes
import Pipes.Concurrent
import qualified Pipes.Prelude as P
import Data.Monoid

{- $intro
    The @pipes-concurrency@ library provides a simple interface for
    communicating between concurrent pipelines.  Use this library if you want
    to:

    * merge multiple streams into a single stream,

    * stream data from a callback \/ continuation,

    * broadcast data,

    * build a work-stealing setup, or

    * implement basic functional reactive programming (FRP).

    For example, let's say that we design a simple game with a single unit's
    health as the global state.  We'll define an event handler that modifies the
    unit's health in response to events:

@
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Maybe
import Pipes

\-\- The game events
data Event = Harm Integer | Heal Integer | Quit

\-\- The game state
type Health = Integer

handler :: () -> 'Consumer' Event (StateT Health (MaybeT IO)) r
handler () = forever $ do
    event  <- 'await' ()
    health <- 'lift' $ do
        case event of
            Harm n -> modify (subtract n)
            Heal n -> modify (+        n)
            Quit   -> mzero
        get
    liftIO $ putStrLn $ \"Health = \" ++ show health
@

    However, we have two concurrent event sources that we wish to hook up to our
    event handler.  One translates user input to game events:

@
user :: () -> 'Producer' Event IO r
user () = forever $ do
    command <- 'lift' getLine
    case command of
        \"potion\" -> 'yield' (Heal 10)
        \"quit\"   -> 'yield'  Quit
        _        -> 'lift' $ putStrLn \"Invalid command\"
@

    ... while the other creates inclement weather:

@
import Control.Concurrent

acidRain :: () -> 'Producer' Event IO r
acidRain () = forever $ do
    'yield' (Harm 1)
    'lift' $ threadDelay 2000000
@

    To merge these sources, we 'spawn' a new FIFO mailbox which we will use to
    merge the two streams of asynchronous events:

@
'spawn' :: 'Buffer' a -> IO ('Input' a, 'Output' a)
@

    'spawn' takes a mailbox 'Buffer' as an argument, and we will specify that we
    want our mailbox to store an 'Unbounded' number of messages:

@
import Pipes.Concurrent

main = do
    (input, output) <- 'spawn' 'Unbounded'
    ...
@

   'spawn' creates this mailbox in the background and then returns two values:

    * an @(Input a)@ that we use to add messages of type @a@ to the mailbox

    * an @(Output a)@ that we use to consume messages of type @a@ from the
      mailbox

    We will be streaming @Event@s through our mailbox, so our @input@ has type
    @(Input Event)@ and our @output@ has type @(Output Event)@.

    To stream @Event@s into the mailbox , we use 'toInput', which writes values
    to the mailbox's 'Input' end:

@
'toInput' :: 'Input' a -> () -> 'Consumer' a IO ()
@

    We can concurrently forward multiple streams to the same 'Input', which
    asynchronously merges their messages into the same mailbox:

@
    ...
    forkIO $ do 'run' $ (acidRain >-> 'toInput' input) ()
                'performGC'  \-\- I'll explain 'performGC' below
    forkIO $ do 'run' $ (user     >-> 'toInput' input) ()
                'performGC'
    ...
@

    To stream @Event@s out of the mailbox, we use 'fromOutput', which reads
    values from the mailbox's 'Output' end:

@
'fromOutput' :: 'Output' a -> () -> 'Producer' a IO ()
@

    We will forward our merged stream to our @handler@ so that it can listen to
    both @Event@ sources:

@
    ...
    'runMaybeT' $ (`'evalStateT'` 100) $ 'run' $
       ('hoist' ('lift' . 'lift') . 'fromOutput' output >-> handler) ()
@

    Our final @main@ becomes:

@
main = do
    (input, output) <- 'spawn' 'Unbounded'
    forkIO $ do 'run' $ (acidRain >-> 'toInput' input) ()
                'performGC'
    forkIO $ do 'run' $ (user     >-> 'toInput' input) ()
                'performGC'
    'runMaybeT' $ (`'evalStateT'` 100) $ 'run' $
       ('hoist' ('lift' . 'lift') . 'fromOutput' output >-> handler) ()
@

    ... and when we run it we get the desired concurrent behavior:

@
$ ./game
Health = 99
Health = 98
potion\<Enter\>
Health = 108
Health = 107
Health = 106
potion\<Enter\>
Health = 116
Health = 115
quit\<Enter\>
$
@
-}

{- $steal
    You can also have multiple pipes reading from the same mailbox.  Messages
    get split between listening pipes on a first-come first-serve basis.

    For example, we'll define a \"worker\" that takes a one-second break each
    time it receives a new job:

@
import Control.Concurrent
import Control.Monad
import Pipes

worker :: (Show a) => Int -> () -> 'Consumer' a IO r
worker i () = forever $ do
    a <- 'await' ()
    'lift' $ threadDelay 1000000  \-\- 1 second
    'lift' $ putStrLn $ \"Worker #\" ++ show i ++ \": Processed \" ++ show a
@

    Fortunately, these workers are cheap, so we can assign several of them to
    the same job:

@
import Control.Concurrent.Async
import qualified Pipes.Prelude as P
import Pipes.Concurrent

main = do
    (input, output) <- 'spawn' 'Unbounded'
    as <- 'forM' [1..3] $ \i ->
          async $ do 'run' $ ('fromOutput' output >-> worker i) ()
                     'performGC'
    a  <- async $ do 'run' $ (P.fromList [1..10] >-> 'toInput' input) ()
                     'performGC'
    mapM_ 'wait' (a:as)
@

    The above example uses @Control.Concurrent.Async@ from the @async@ package
    to fork each thread and wait for all of them to terminate:

@
$ ./work
Worker #2: Processed 3
Worker #1: Processed 2
Worker #3: Processed 1
Worker #3: Processed 6
Worker #1: Processed 5
Worker #2: Processed 4
Worker #2: Processed 9
Worker #1: Processed 8
Worker #3: Processed 7
Worker #2: Processed 10
$
@

    What if we replace 'P.fromList' with a different source that reads lines
    from user input until the user types \"quit\":

@
user :: () -> 'Producer' String IO ()
user = P.stdin >-> P.takeWhile (/= \"quit\")

main = do
    (input, output) <- 'spawn' 'Unbounded'
    as <- 'forM' [1..3] $ \i ->
          async $ do 'run' $ ('fromOutput' output >-> worker i) ()
                     'performGC'
    a  <- async $ do 'run' $ (user >-> 'toInput' input) ()
                     'performGC'
    mapM_ 'wait' (a:as)
@

    This still produces the correct behavior:

@
$ ./work
Test\<Enter\>
Worker #1: Processed \"Test\"
Apple\<Enter\>
Worker #2: Processed \"Apple\"
42\<Enter\>
Worker #3: Processed \"42\"
A\<Enter\>
B\<Enter\>
C\<Enter\>
Worker #1: Processed \"A\"
Worker #2: Processed \"B\"
Worker #3: Processed \"C\"
quit\<Enter\>
$
@
-}

{- $termination

    Wait...  How do the workers know when to stop listening for data?  After
    all, anything that has a reference to 'Input' could potentially add more
    data to the mailbox.

    It turns out that 'fromOutput' is smart and only terminates when the
    upstream 'Input' is garbage collected.  'fromOutput' builds on top of the
    more primitive 'recv' command, which returns a 'Nothing' when the 'Input' is
    garbage collected:

@
'recv' :: 'Output' a -> 'STM' (Maybe a)
@

    Otherwise, 'recv' blocks if the mailbox is empty since it assumes that if
    the 'Input' has not been garbage collected then somebody might still produce
    more data.

    Does it work the other way around?  What happens if the workers go on strike
    before processing the entire data set?

@
    ...
    as <- 'forM' [1..3] $ \i ->
          \-\- Each worker refuses to process more than two values
          async $ do 'run' $
                         ('fromOutput' output >-> P.take 2 >-> worker i) ()
                     'performGC'
    ...
@

    Let's find out:

@
$ ./work
How\<Enter\>
Worker #1: Processed \"How\"
many\<Enter\>
roads\<Enter\>
Worker #2: Processed \"many\"
Worker #3: Processed \"roads\"
must\<Enter\>
a\<Enter\>
man\<Enter\>
Worker #1: Processed \"must\"
Worker #2: Processed \"a\"
Worker #3: Processed \"man\"
walk\<Enter\>
$
@

    'toInput' similarly shuts down when the 'Output' is garbage collected,
    preventing the user from submitting new values.  'toInput' builds on top of
    the more primitive 'send' command, which returns a 'False' when the 'Output'
    is garbage collected:

@
'send' :: 'Input' a -> a -> 'STM' Bool
@

    Otherwise, 'send' blocks if the mailbox is full, since it assumes that if
    the 'Output' has not been garbage collected then somebody could still
    consume a value from the mailbox, making room for a new value.

    This is why we have to insert 'performGC' calls whenever we release a
    reference to either the 'Input' or 'Output'.  Without these calls we cannot
    guarantee that the garbage collector will trigger and notify the opposing
    end if the last reference was released.

    You can also opt to not use 'performGC' at all.  This is preferable for
    long-running programs and it is completely safe.  When you omit the
    'performGC' call you simply delay garbage collecting mailboxes until the
    next garbage collection cycle.  However, this tutorial will continue to use
    `performGC` since all the examples are short-lived programs that need to
    terminate promptly.
-}

{- $mailbox
    So far we haven't observed 'send' blocking because we only 'spawn'ed
    'Unbounded' mailboxes.  However, we can control the size of the mailbox to
    tune the coupling between the 'Input' and the 'Output' ends.

    If we set the mailbox 'Buffer' to 'Single', then the mailbox holds exactly
    one message, forcing synchronization between 'send's and 'recv's.  Let's
    observe this by sending an infinite stream of values, logging all values to
    'stdout':

@
main = do
    (input, output) <- 'spawn' 'Single'
    as <- 'forM' [1..3] $ \i ->
          async $ do 'run' $
                         ('fromOutput' output >-> P.take 2 >-> worker i) ()
                     'performGC'
    a  <- async $ do
        'run' $ (P.fromList [(1::Int)..] >-> P.tee P.print >-> 'toInput' input) ()
        'performGC'
    mapM_ 'wait' (a:as)
@

    The 7th value gets stuck in the mailbox, and the 8th value blocks because
    the mailbox never clears the 7th value:

@
$ ./work
1
2
3
4
5
Worker #3: Processed 3
Worker #2: Processed 2
Worker #1: Processed 1
6
7
8
Worker #1: Processed 6
Worker #2: Processed 5
Worker #3: Processed 4
$
@

    Contrast this with an 'Unbounded' mailbox for the same program, which keeps
    accepting values until downstream finishes processing the first six values:

@
$ ./work
1
2
3
4
5
6
7
8
9
...
487887
487888
Worker #3: Processed 3
Worker #2: Processed 2
Worker #1: Processed 1
487889
487890
...
969188
969189
Worker #1: Processed 6
Worker #2: Processed 5
Worker #3: Processed 4
969190
969191
$
@

    You can also choose something in between by using a 'Bounded' mailbox which
    caps the mailbox size to a fixed value.  Use 'Bounded' when you want mostly
    loose coupling but still want to guarantee bounded memory usage:

@
main = do
    (input, output) <- 'spawn' ('Bounded' 100)
    ...
@

@
$ ./work
...
103
104
Worker #3: Processed 3
Worker #2: Processed 2
Worker #1: Processed 1
105
106
107
Worker #1: Processed 6
Worker #2: Processed 5
Worker #3: Processed 4
$
@
-}

{- $broadcast
    You can also broadcast data to multiple listeners instead of dividing up the
    data.  Just use the 'Monoid' instance for 'Input' to combine multiple
    'Input' ends together into a single broadcast 'Input':

@
import Control.Monad
import Control.Concurrent.Async
import Pipes
import Pipes.Concurrent
import qualified Pipes.Prelude as P
import Data.Monoid

main = do
    (input1, output1) <- 'spawn' 'Unbounded'
    (input2, output2) <- 'spawn' 'Unbounded'
    a1 <- async $ do
        'run' $ (P.stdin >-> 'toInput' (input1 <> input2)) ()
        'performGC'
    as <- forM [output1, output2] $ \output -> async $ do
        'run' $ ('fromOutput' output >-> P.take 2 >-> P.stdout) ()
        'performGC'
    mapM_ 'wait' (a1:as)
@

    In the above example, 'P.stdin' will broadcast user input to both mailboxes,
    and each mailbox forwards its values to 'P.stdout', echoing the message to
    standard output:

@
$ ./broadcast
ABC\<Enter\>
ABC
ABC
DEF\<Enter\>
DEF
DEF
GHI\<Enter\>
$ 
@

    The combined 'Input' stays alive as long as any of the original 'Input's
    remains alive.  In the above example, 'toInput' terminates on the third
    'send' attempt because it detects that both listeners died after receiving
    two messages.

    Use 'mconcat' to broadcast to a list of 'Input's, but keep in mind that you
    will incur a performance price if you combine thousands of 'Input's or more
    because they will create a very large 'STM' transaction.  You can improve
    performance for very large broadcasts if you sacrifice atomicity and
    manually combine multiple 'send' actions in 'IO' instead of 'STM'.
-}

{- $updates
    Sometimes you don't want to handle every single event.  For example, you
    might have an input and output device (like a mouse and a monitor) where the
    input device updates at a different pace than the output device

@
import Control.Concurrent
import Pipes
import qualified Pipes.Prelude as P

\-\- Fast input updates
inputDevice :: (Monad m) => () -> 'Producer' Integer m ()
inputDevice = P.fromList [1..]

\-\- Slow output updates
outputDevice :: () -> 'Consumer' Integer IO r
outputDevice () = forever $ do
    n <- 'await' ()
    'lift' $ do
        print n
        threadDelay 1000000
@

    In this scenario you don't want to enforce a one-to-one correspondence
    between input device updates and output device updates because you don't
    want either end to block waiting for the other end.  Instead, you just need
    the output device to consult the 'Latest' value received from the 'Input':

@
import Control.Concurrent.Async
import Pipes.Concurrent

main = do
    (input, output) <- 'spawn' (Latest 0)
    a1 <- async $ do
        'run' $ (inputDevice >-> 'toInput' input) ()
        'performGC'
    a2 <- async $ do
        'run' $ ('fromOutput' output >-> P.take 5 >-> outputDevice) ()
        'performGC'
    mapM_ 'wait' [a1, a2]
@

    'Latest' selects a mailbox that always stores exactly one value.  The
    'Latest' constructor takes a single argument (@0@, in the above example)
    specifying the starting value to store in the mailbox.  'send' overrides the
    currently stored value and 'recv' peeks at the latest stored value without
    consuming it.  In the above example the @outputDevice@ periodically peeks at    the latest value stashed inside the mailbox:

@
$ ./peek
5
752452
1502636
2248278
2997705
$
@

    A 'Latest' mailbox is never empty because it begins with a default value and
    'recv' never removes the value from the mailbox.  A 'Latest' mailbox is also
    never full because 'send' always succeeds, overwriting the previously stored
    value.
-}

{- $callback
    @pipes-concurrency@ also solves the common problem of getting data out of a
    callback-based framework into @pipes@.

    For example, suppose that we have the following callback-based function:

@
import Control.Monad

onLines :: (String -> IO a) -> IO b
onLines callback = forever $ do
    str <- getLine
    callback str
@

    We can use 'send' to free the data from the callback and then we can
    retrieve the data on the outside using 'fromOutput':

@
import Pipes
import Pipes.Concurrent
import qualified Pipes.Prelude as P

onLines' :: () -> 'Producer' String IO ()
onLines' () = do
    (input, output) <- 'lift' $ 'spawn' 'Single'
    'lift' $ forkIO $ onLines (\str -> atomically $ 'send' input str)
    'fromOutput' output ()

main = 'run' $ (onLines' >-> P.takeWhile (/= \"quit\") >-> P.stdout) ()
@

    Now we can stream from the callback as if it were an ordinary 'Producer':

@
$ ./callback
Test\<Enter\>
Test
Apple\<Enter\>
Apple
quit\<Enter\>
$
@

-}

{- $safety
    @pipes-concurrency@ avoids deadlocks because 'send' and 'recv' always
    cleanly return before triggering a deadlock.  This behavior works even in
    complicated scenarios like:

    * cyclic graphs of connected mailboxes,

    * multiple readers and multiple writers to the same mailbox, and

    * dynamically adding or garbage collecting mailboxes.

    The following example shows how @pipes-concurrency@ will do the right thing
    even in the case of cycles:

@
import Control.Concurrent.Async
import Pipes
import Pipes.Concurrent
import qualified Pipes.Prelude as P

main = do
    (in1, out1) <- 'spawn' 'Unbounded'
    (in2, out2) <- 'spawn' 'Unbounded'
    a1 <- async $ do
        'run' $ ((P.fromList [1,2] >=> 'fromOutput' out1) >-> 'toInput' in2) ()
        'performGC'
    a2 <- async $ do
        'run' $ ('fromOutput' out2 >-> P.tee P.print >-> P.take 6 >-> 'toInput' in1) ()
        'performGC'
    mapM_ 'wait' [a1, a2]
@

    The above program jump-starts a cyclic chain with two input values and
    terminates one branch of the cycle after six values flow through.  Both
    branches correctly terminate and get garbage collected without triggering
    deadlocks when 'takeB_' finishes:

@
$ ./cycle
1
2
1
2
1
2
$
@

-}

{- $conclusion
    @pipes-concurrency@ adds an asynchronous dimension to @pipes@.  This
    promotes a natural division of labor for concurrent programs:

    * Fork one pipeline per deterministic behavior

    * Communicate between concurrent pipelines using @pipes-concurrency@

    This promotes an actor-style approach to concurrent programming where
    pipelines behave like processes and mailboxes behave like ... mailboxes.

    You can ask questions about @pipes-concurrency@ and other @pipes@ libraries
    on the official @pipes@ mailing list at
    <mailto:haskell-pipes@googlegroups.com>.
-}

{- $appendix
    I've provided the full code for the above examples here so you can easily
    try them out:

@
\-\- game.hs

import Control.Concurrent
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Maybe
import Pipes
import Pipes.Concurrent

\-\- The game events
data Event = Harm Integer | Heal Integer | Quit

\-\- The game state
type Health = Integer

handler :: () -> Consumer Event (StateT Health (MaybeT IO)) r
handler () = forever $ do
    event  <- await ()
    health <- lift $ do
        case event of
            Harm n -> modify (subtract n)
            Heal n -> modify (+        n)
            Quit   -> mzero
        get
    liftIO $ putStrLn $ \"Health = \" ++ show health

user :: () -> Producer Event IO r
user () = forever $ do
    command <- lift getLine
    case command of
        \"potion\" -> yield (Heal 10)
        \"quit\"   -> yield  Quit
        _        -> lift $ putStrLn \"Invalid command\"

acidRain :: () -> Producer Event IO r
acidRain () = forever $ do
    yield (Harm 1)
    lift $ threadDelay 2000000

main = do
    (input, output) <- spawn Unbounded
    forkIO $ do run $ (acidRain >-> toInput input) ()
                performGC
    forkIO $ do run $ (user     >-> toInput input) ()
                performGC
    runMaybeT $ (`evalStateT` 100) $ run $
       (hoist (lift . lift) . fromOutput output >-> handler) ()

\-\- work.hs
 
import Control.Concurrent
import Control.Concurrent.Async
import Control.Monad
import Pipes
import Pipes.Concurrent
import qualified Pipes.Prelude as P

worker :: (Show a) => Int -> () -> Consumer a IO r
worker i () = forever $ do
    a <- await ()
    lift $ threadDelay 1000000
    lift $ putStrLn $ \"Worker #\" ++ show i ++ \": Processed \" ++ show a

user :: () -> Producer String IO ()
user = P.stdin >-> P.takeWhile (/= \"quit\")

main = do
    let buffer1 = Unbounded
        buffer2 = Single
        buffer3 = Bounded 100
    (input, output) <- spawn buffer1
    let consumer1 i =              worker i
        consumer2 i = P.take 2 >-> worker i
    as <- forM [1..3] $ \i -> async $ do
        run $ (fromOutput output >-> consumer1 i) ()
        performGC
    let producer1 = P.fromList [1..10]
        producer2 = user
        producer3 = P.fromList [1..] >-> P.tee P.print
    a  <- async $ do run $ (producer1 >-> toInput input) ()
                     performGC

    mapM_ wait (a:as)

\-\- peek.hs

import Control.Concurrent
import Control.Concurrent.Async
import Pipes
import Pipes.Concurrent
import qualified Pipes.Prelude as P

\-\- Fast input updates
inputDevice :: (Monad m) => () -> Producer Integer m ()
inputDevice = P.fromList [1..]

\-\- Slow output updates
outputDevice :: () -> Consumer Integer IO r
outputDevice () = forever $ do
    n <- await ()
    lift $ do
        print n
        threadDelay 1000000

main = do
    (input, output) <- spawn (Latest 0)
    a1 <- async $ do
        run $ (inputDevice >-> toInput input) ()
        performGC
    a2 <- async $ do
        run $ (fromOutput output >-> P.take 5 >-> outputDevice) ()
        performGC
    mapM_ wait [a1, a2]

\-\- callback.hs

import Control.Monad
import Pipes
import Pipes.Concurrent
import qualified Pipes.Prelude as P

onLines :: (String -> IO a) -> IO b
onLines callback = forever $ do
    str <- getLine
    callback str

onLines :: () -> Producer String IO ()
onLines () = do
    (input, output) <- lift $ spawn Single
    lift $ forkIO $ onLines (\str -> atomically $ send input str)
    fromOutput output ()

main = run $ (onLines >-> P.takeWhile (/= \"quit\") >-> P.stdout) ()
@
-}
