
module Control.Distributed.Viewstamped where
import Control.Distributed.Viewstamped.Replica

import Data.Maybe
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Control.Concurrent
import Control.Monad.State
import Control.Monad.Trans
import Control.Concurrent.STM

data Status = Normal | ViewChange | Recovering deriving (Show, Eq)
type OpNumber = Integer
type ViewNumber = Integer
type RequestNumber = Integer
type CommitNumber = Integer
type ReplicaNumber = Integer
type Nonce = Integer -- DO SOMETHING ELSE LATER
type ClientID = String

type RequestStatus = Ordering

type Key = String
type Value = String
data Op = Get Key | Set Key Value  deriving (Show, Eq)
data Operation = Operation ClientRequest Op deriving (Show, Eq)
type OpLog = [Operation]

-- Config items represent individual nodes inside the full
-- configuration, [Config], which should be the same in every
-- node
data Config = IP String |
              Socket String |
              Local String |
              ToClient ClientID |
              Placeholder deriving (Show, Eq, Ord)

-- Input and output types for state machine
data Event = ReceivedMessage Config Message |
             SentMessage Config Message |
             Defer Event |
             SendCommitTimeoutReached |
             PrimaryContactTimeoutReached |
             StartedRecovery deriving (Show, Eq)

-- Messages!
-- Stuff going between the nodes. Any intra-node stuff
-- (inspecting state at time or w/e) should probably just be
-- functions that act on the entire replica structure
data Message = Request ClientRequest |
               Prepare ViewNumber ClientRequest OpNumber CommitNumber |
               PrepareOK ViewNumber OpNumber ReplicaNumber |
               Reply ViewNumber RequestNumber Response |
               Commit ViewNumber CommitNumber |
               StartViewChange ViewNumber ReplicaNumber |
               -- When replica i receives STARTVIEWCHANGE messages for its view-number from f other replicas, it
               -- sends a DOVIEWCHANGE v, l, v’, n, k message
               -- to the node that will be the primary in the new view.
               -- Here v is its view-number, l is its log, v' is the view
               -- number of the latest view in which its status was
               -- normal, n is the op-number, and k is the commitnumber.
               DoViewChange ViewNumber OpLog ViewNumber OpNumber CommitNumber ReplicaNumber |
               -- STARTVIEW v, l, n, k messages to
               -- the other replicas, where l is the new log, n is the
               -- op-number, and k is the commit-number.
               StartView ViewNumber OpLog OpNumber CommitNumber |
               -- The recovering replica, i, sends a RECOVERY i, x
               -- message to all other replicas, where x is a nonce.
               Recovery ReplicaNumber Nonce |
               -- RECOVERYRESPONSE v, x, l, n, k, j message to the recovering replica
               -- where v is its viewnumber and x is the nonce in the RECOVERY message.
               -- If j is the primary of its view, l is its log, n is
               -- its op-number, and k is the commit-number; otherwise these values are nil.
               -- [j is replica number of respondant]
               RecoveryResponse ViewNumber Nonce (Maybe OpLog) (Maybe OpNumber) (Maybe CommitNumber) ReplicaNumber
        deriving (Show, Eq)

-- ClientRequest separate due to being in Request and Prepare messsages
data ClientRequest = ClientRequest Op ClientID RequestNumber deriving (Show, Eq)
toOperation :: ClientRequest -> Operation
toOperation cr@(ClientRequest op cId _) = Operation cr op

data Response = Got Key (Maybe Value) | SetOK | SetNotOk deriving (Show, Eq)

-- Data type containing core replica fields
data ReplicaCore = ReplicaCore {
    configuration :: [Config],
    replicaNumber :: Integer,
    viewNumber :: ViewNumber,
    store :: Map String String,
    opNumber :: OpNumber,
    opLog :: OpLog,
    commitNumber :: OpNumber,
    clientTable :: Map ClientID (ClientRequest, Response)
} deriving (Show, Eq)

primary :: ReplicaCore -> ReplicaNumber
primary rc = fromIntegral $ (viewNumber rc) `mod` (fromIntegral $ length $ configuration rc)

-- Data type describing replica in each status
-- NOTE: This is actually an extension/mutation of
-- the normal, where status can be NORMAL, VIEWCHANGE, or RECOVERING
-- *** What about multiple PrepareOK rounds? Is there a better way to handle them
-- than deferring prepareoks that don't match?
data Replica = Primary { core :: ReplicaCore } |
               AwaitingPrepareOKs { core :: ReplicaCore, required :: Int, prepares :: Map Config Bool } |
               Backup { core :: ReplicaCore } |
               -- Messages stored in maps for lookup (could do this a different way but idk)
               -- so far only important for DoViewChanges, but maybe important elsewhere?
               AwaitingStartViewChanges { core :: ReplicaCore, required :: Int, viewChanges :: Map Config Message, latestNormal :: ViewNumber } |
               AwaitingDoViewChanges { core :: ReplicaCore, required :: Int, viewChanges :: Map Config Message } |
               AwaitingStartView { core :: ReplicaCore } | 
               AwaitingRecoveryResponses { core :: ReplicaCore, required :: Int, primaryViewNumber :: Maybe ViewNumber, viewChanges :: Map Config Message, latestNormal :: ViewNumber }
               deriving (Show, Eq)

-- *** IS THIS THE RIGHT APPROACH?
-- Status could be explicitly represented on replicas instead?
replicaStatus :: Replica -> Status
replicaStatus (Primary _) = Normal
replicaStatus (Backup _) = Normal
replicaStatus (AwaitingPrepareOKs _ _ _) = Normal
replicaStatus (AwaitingStartViewChanges _ _ _ _) = ViewChange
replicaStatus (AwaitingRecoveryResponses _ _ _ _ _) = Recovering

isPrimary :: Replica -> Bool
isPrimary (Primary _) = True
isPrimary _ = False

awaitPrepareOKs :: ReplicaCore -> ClientRequest -> ([Event], Replica)
awaitPrepareOKs rc cr = (msgs , AwaitingPrepareOKs rc requiredResponses M.empty)
    where numNodes = length $ configuration rc
          requiredResponses = ceiling $ ((fromIntegral numNodes) - 1) / 2
          msg = Prepare (viewNumber rc) cr (opNumber rc) (commitNumber rc)
          msgs = map (\x -> SentMessage x msg) (configuration rc)

data Client = Client {
    clientId :: ClientID,
    config :: [Config],
    viewNum :: ViewNumber,
    requestNum :: RequestNumber
}

step :: Replica -> Event -> ([Event], Replica)
-- # NORMAL
-- ## Normal 1
-- The client sends a REQUEST operation, clientId, requestNumber
-- message to the primary
-- HAPPENS ON CLIENT-SIDE
-- ## Normal 2-3
-- Primary receives request, tests request, updates client table,
-- and sends PREPARE messages to backups if valid.
-- Return values will be:
-- ([], Primary), ([client response], Primary), or ([prepares], AwaitingPrepareOKs)
step r@(Primary rc) e@(ReceivedMessage _ (Request _)) = clientRequest r e
step r@(Backup _) (ReceivedMessage _ (Request _)) = ([], r)
-- ## Normal 4
-- Backups process PREPARE messages in order: a
-- backup won’t accept a prepare with op-number n
-- until it has entries for all earlier requests in its log.
-- When a backup i receives a PREPARE message, it
-- waits until it has entries in its log for all earlier 
-- requests (doing state transfer if necessary to get the
-- missing information). 
step (Backup rc) e@(ReceivedMessage _ (Prepare _ _ _ _)) = receivePrepare rc e
-- ## Normal 5
-- The primary waits for f PREPAREOK messages
-- from different backups; at this point it considers
-- the operation (and all earlier ones) to be committed.
-- * If prepared > 1, add it to the stack
-- * If prepared == 1, COMMIT and send REPLY
-- Then it sends a REPLY v, s, x message to the client
step r@(AwaitingPrepareOKs _ _ _) m@(ReceivedMessage _ (PrepareOK _ _ _)) = applyPrepareOKGuard r m
-- ## Normal 6
-- Normally the primary informs backups about the
-- commit when it sends the next PREPARE message;
-- this is the purpose of the commit-number in the
-- PREPARE message. However, if the primary does
-- not receive a new client request in a timely way, it
-- instead informs the backups of the latest commit by
-- sending them a COMMIT v, k message
step r@(Primary rc) e@SendCommitTimeoutReached = commitTimeout r e
-- ## Normal 7
-- When a backup learns of a commit, it waits 
-- until it has the request in its log (which may require
-- state transfer) and until it has executed all earlier
-- operations. Then it executes the operation by performing 
-- the up-call to the service code, increments
-- its commit-number, updates the client’s entry in the
-- client-table, but does not send the reply to the client
step r@(Backup _) e@(ReceivedMessage _ (Commit _ _)) = applyCommit r e


-- # VIEW CHANGE
-- ## View Change 1
-- A replica i that notices the need for a view change
-- advances its view-number, sets its status to viewchange,
-- and sends a STARTVIEWCHANGE v, i message to the all other
-- replicas. A replica notices the need for a view change
-- either based on its own timer, or because it receives a 
-- STARTVIEWCHANGE or DOVIEWCHANGE message for a view with a
-- larger number than its own view-number
step r@(Backup _) e@SendCommitTimeoutReached = motivateViewChange r e
-- Monitor StartViewChange
step r@(Primary _) e@(ReceivedMessage _ (StartViewChange _ _)) = motivateViewChange r e
step r@(Backup _) e@(ReceivedMessage _ (StartViewChange _ _)) = motivateViewChange r e
step r@(AwaitingPrepareOKs _ _ _) e@(ReceivedMessage _ (StartViewChange _ _)) = motivateViewChange r e
-- Monitor DoViewChange
step r@(Primary rc) e@(ReceivedMessage _ (DoViewChange _ _ _ _ _ _)) = motivateViewChange r e
step r@(Backup rc) e@(ReceivedMessage _ (DoViewChange _ _ _ _ _ _)) = motivateViewChange r e
step r@(AwaitingPrepareOKs _ _ _) e@(ReceivedMessage _ (DoViewChange _ _ _ _ _ _)) = motivateViewChange r e

-- ## View Change 2
-- When replica i receives STARTVIEWCHANGE messages
-- for its view-number from f other replicas, it
-- sends a DOVIEWCHANGE v, l, v’, n, k, ii message
-- to the node that will be the primary in the new view
-- *** Cases for other nodes covered as part of step 1. 
step r@(AwaitingStartViewChanges _ _ _ _) m@(ReceivedMessage _ (StartViewChange _ _)) = applyStartViewChange r m
-- ## View Change 3
--  When the new primary receives f + 1
-- DOVIEWCHANGE messages from different
-- replicas (including itself), it sets its view-number
-- to that in the messages and selects as the new log
-- the one contained in the message with the largest v'
step r@(AwaitingStartView _) e@(ReceivedMessage _ (DoViewChange _ _ _ _ _ _)) = applyDoViewChange r e
-- ## View Change 4 (and 3, combined)
-- The new primary starts accepting client requests. It
-- also executes (in order) any committed operations
-- that it hadn’t executed previously, updates its client
-- table, and sends the replies to the clients.
step r@(AwaitingDoViewChanges _ _ _) e@(ReceivedMessage _ (StartViewChange _ _)) = applyDoViewChange r e
-- ## View Change 5
-- When other replicas receive the STARTVIEW message,
-- they replace their log with the one in the message,
-- set their op-number to that of the latest entry
-- in the log, set their view-number to the view number
-- in the message, change their status to normal,
-- and update the information in their client-table
step r@(AwaitingStartView rc) m@(ReceivedMessage _ (StartView _ _ _ _)) = applyStartView r m

-- ## RECOVERY
-- ### Recovery 1
-- While a replica’s status is recovering it does not
-- participate in either the request processing protocol 
-- or the view change protocol.
-- *** Any replica can enter recovery (represented here by an event)
step r m@StartedRecovery = startRecovery r m
-- ### Recovery 2
-- A replica j replies to a RECOVERY message only
-- when its status is normal. In this case the replica
-- sends a RECOVERYRESPONSE v, x, l, n, k, j message to
-- the recovering replica
-- *** Could check status, doing it this way to not run afoul of later pattern checks
step r@(Primary _) m@(ReceivedMessage _ (Recovery _ _)) = respondToRecovery r m
step r@(Backup _) m@(ReceivedMessage _ (Recovery _ _)) = respondToRecovery r m
step r@(AwaitingPrepareOKs _ _ _) m@(ReceivedMessage _ (Recovery _ _)) = respondToRecovery r m
-- ### Recovery 3
-- The recovering replica waits to receive at least f + 1
-- RECOVERYRESPONSE messages from different replicas, all
-- containing the nonce it sent in its RECOVERY message,
-- including one from the primary
-- of the latest view it learns of in these messages.
-- Then it updates its state using the information from
-- the primary, changes its status to normal, and the
-- recovery protocol is complete.
step r@(AwaitingRecoveryResponses _ _ _ _ _) m@(ReceivedMessage _ (Recovery _ _)) = applyRecoveryResponse r m

clientRequest :: Replica -> Event -> ([Event], Replica)
clientRequest r@(Primary rep) (ReceivedMessage _ (Request cr@(ClientRequest op cId reqNum))) = do
    -- ## Normal 2
    -- When the primary receives the request, it compares
    -- the request-number in the request with the information
    -- in the client table. If the request-number s isn’t
    -- bigger than the information in the table it drops the
    -- request, but it will re-send the response if the request 
    -- is the most recent one from this client and it
    -- has already been executed.
    let (o, mRes) = testRecent rep cr
    case o of
        LT -> ([], r) -- Request is old, ignore
        EQ -> do
            msg <- pure $ SentMessage (ToClient cId) $ Reply (viewNumber rep) reqNum (fromJust mRes)
            ([msg], r) -- Request is most recent, send saved response
        GT -> do
            -- ## Normal 3
            -- The primary advances op-number, adds the request
            -- to the end of the log,
            operation <- pure $ toOperation cr
            rep <- pure $ pushOp rep operation
            -- Apply the op to internal state and generate a response if applicable
            (newRC, res) <- pure $ apply rep operation
            -- Update the client table
            newRC <- pure $ updateClientTable newRC cId cr res
            -- Create Prepares, await PrepareOKs
            waitingRep <- awaitPrepareOKs newRC cr
            ([], waitingRep)


receivePrepare :: ReplicaCore -> Event -> ([Event], Replica)
receivePrepare rc e@(ReceivedMessage sender m@(Prepare vn cr op cn)) = do
    -- *** WHAT ABOUT PREPARE REQUESTS OUT OF ORDER?
    -- IS DEFERRING REALLY ENOUGH?
    -- SHOULD EARLIER PREPARE REQUESTS BREAK PROCESS?
    if sender /= prim then ([], Backup rc) else -- Bail if sender is not recognized as current primary
        if vn /= (viewNumber rc) then ([], Backup rc) else
            case compare op currentOp of -- Bail if view number is different
                LT -> ([], Backup rc) -- Ignore! (maybe)
                EQ -> applyPrepare rc m
                GT -> ([Defer e], Backup rc)
    where prim = (configuration rc) !! (fromIntegral $ primary rc)
          currentOp = (opNumber rc) + 1

applyPrepare :: ReplicaCore -> Message -> ([Event], Replica)
-- Then it increments its op
-- number, adds the request to the end of its log, 
-- updates the client’s information in the client-table, and
-- sends a PREPAREOK v, n, ii message to the primary to indicate
-- that this operation and all earlier
-- ones have prepared locally.
-- *** I think commits on backup nodes also happen here ? **
-- Normally the primary informs backups about the
-- commit when it sends the next PREPARE message;
-- this is the purpose of the commit-number in the
-- PREPARE message.
-- *******************
applyPrepare rc (Prepare vn cr@(ClientRequest _ _ _) op cn) = do
        operation <- pure $ toOperation cr
        rc <- pure $ pushOp rc operation
        msg <- pure $ PrepareOK vn op (replicaNumber rc)
        prim <- pure $ (configuration rc) !! (fromIntegral $ primary rc)
        case shouldCommit of
            False -> ([SentMessage prim msg], Backup rc)
            True -> do
                rc <- pure $ applyOperations rc cn
                ([SentMessage prim msg], Backup rc) 
    where shouldCommit = cn > (commitNumber rc)

applyPrepareOKGuard :: Replica -> Event -> ([Event], Replica)
-- Check incoming ApplyPrepareOK messages for safety/consistency.
-- Defer messages referring to different operations
-- Throw out duplicate messages
-- Only process new messages referring to current operation
applyPrepareOKGuard r@(AwaitingPrepareOKs rc reqd prepMap) e@(ReceivedMessage cfg (PrepareOK vn on rn)) = case vn /= (viewNumber rc) of
    True -> ([], r) -- Message refers to different view, discard
    False -> case compare on (opNumber rc) of
        LT -> ([], r) -- Message refers to previous operation, discard
        GT -> ([Defer e], r) -- Message refers to subsequent operation, defer -- Wait. Maybe this is super wrong. Potentially just discard. IDK.
        EQ -> case M.lookup cfg prepMap of
            Just _ -> ([], r) -- Message is a duplicate. We already received one from this node. Discard.
            Nothing -> applyPrepareOK r e

applyPrepareOK :: Replica -> Event -> ([Event], Replica)
-- The primary waits for f PREPAREOK messages
-- from different backups; at this point it considers
-- the operation (and all earlier ones) to be committed.
applyPrepareOK r@(AwaitingPrepareOKs _ 1 _) m@(ReceivedMessage cfg (PrepareOK _ _ _)) = commit r m
applyPrepareOK r@(AwaitingPrepareOKs rc reqd prepMap) m@(ReceivedMessage cfg (PrepareOK vn on rn)) = do
        prepMap <- pure $ M.insert cfg True prepMap
        ([], AwaitingPrepareOKs rc (reqd - 1) prepMap)

applyCommit :: Replica -> Event -> ([Event], Replica)
-- When a backup learns of a commit, it waits until it 
-- has the request in its log (which may require
-- state transfer) and until it has executed all earlier
-- operations. Then it executes the operation by performing 
-- the up-call to the service code, increments
-- its commit-number, updates the client’s entry in the
-- client-table, but does not send the reply to the client
applyCommit r@(Backup rc) e@(ReceivedMessage _ (Commit vn cn)) = case vn == (viewNumber rc) of
    False -> ([], r) -- Refers to different view. Discard
    True -> ([], Backup $ applyOperations rc cn)

applyOperation :: ReplicaCore -> Operation -> ReplicaCore
applyOperation rc op@(Operation cr@(ClientRequest _ cId _) _) = rc{ clientTable = newCTable }
    where (nRc, res) = apply rc op
          newCTable = M.insert cId (cr, res) (clientTable rc)

applyOperations :: ReplicaCore -> CommitNumber -> ReplicaCore
applyOperations rc cn = newCore { commitNumber = cn }
    where oldCN = commitNumber rc
          diff = fromIntegral $ cn - oldCN
          applyable = take diff $ drop (fromIntegral $ commitNumber rc) (opLog rc)
          newCore = foldl applyOperation rc applyable

motivateViewChange :: Replica -> Event -> ([Event], Replica)
-- A replica i that notices the need for a view change
-- advances its view-number, sets its status to viewchange, 
-- and sends a STARTVIEWCHANGE v, ii
-- message to the all other replicas, where v identifies the new view.
motivateViewChange r SendCommitTimeoutReached = startViewChange (core r) ((viewNumber $ core r) + 1)
motivateViewChange r (ReceivedMessage _ (StartViewChange vn rn)) = case vn > (viewNumber $ core r) of
    False -> ([], r) -- ViewNumber not higher. Ignore
    True -> startViewChange (core r) vn
motivateViewChange r (ReceivedMessage _ (DoViewChange vn _ _ _ _ _)) = case vn > (viewNumber $ core r) of
    False -> ([], r) -- ViewNumber not higher. Ignore
    True -> startViewChange (core r) vn


startViewChange :: ReplicaCore -> ViewNumber -> ([Event], Replica)
startViewChange rc vn = do
    rc <- pure $ rc{ viewNumber = vn }
    recd <- pure $ ceiling $ ((fromIntegral $ length $ configuration rc) - 1) / 2
    newReplica <- pure $ AwaitingStartViewChanges rc recd M.empty (viewNumber rc)
    msgs <- pure $ map (\c -> SentMessage c (StartViewChange (viewNumber rc) (replicaNumber rc))) (configuration rc)
    (msgs, newReplica)

selectLatest :: [Message] -> Message
selectLatest doViews = foldl (\m@(DoViewChange _ _ newestNormal on _ _) new@(DoViewChange _ _ newestNormal' on' _ _) -> if newestNormal' > newestNormal || (newestNormal' == newestNormal && on' > on) then new else m) (head doViews) doViews

applyDoViewChange :: Replica -> Event -> ([Event], Replica)
-- First encounter with this message by an AwaitingStartView replica. This replica is the new primary! (maybe)
applyDoViewChange r@(AwaitingStartView rc) e@(ReceivedMessage cfg m@(DoViewChange vn opLog newestNormal on cn rn)) = case newestNormal > (viewNumber rc) of
    False -> ([], r) -- Ignore! (Maybe this is wrong. Should I allow backward view changes?)
    True -> do
        recd <- pure $ ceiling $ ((fromIntegral $ length $ configuration rc) - 1) / 2
        theMap <- pure $ M.insert cfg m M.empty
        r <- pure $ AwaitingDoViewChanges rc (recd - 1) theMap
        ([], r)
applyDoViewChange r@(AwaitingDoViewChanges rc rcd receivedMap) e@(ReceivedMessage sender m@(DoViewChange vn opLog newestNormal on cn rn)) = case shouldExecute of
        False -> case M.lookup sender receivedMap of
            -- Message has been received before. Check to see if it's newer and either overwrite or discard
            Just (DoViewChange vn' opLog' newestNormal' on' cn' rn') -> case newestNormal > newestNormal' || (newestNormal == newestNormal && on > on') of
                True -> do -- New message is more recent.
                    receivedMap <- pure $ M.insert sender m receivedMap
                    ([], r{ viewChanges = receivedMap})
                False -> ([], r) -- New message is not more recent. Ignore.
            Nothing -> do -- Message is new. Add it, decrement required count
                rcd <- pure $ rcd - 1
                receivedMap <- pure $ M.insert sender m receivedMap
                ([], r { required = rcd, viewChanges = receivedMap })
        True -> do
            -- When the new primary receives f + 1
            -- DOVIEWCHANGE messages from different
            -- replicas (including itself), it sets its view-number
            -- to that in the messages and selects as the new log
            -- the one contained in the message with the largest
            -- v' if several messages have the same v' it selects
            -- the one among them with the largest n. It sets its
            -- op-number to that of the topmost entry in the new
            -- log, sets its commit-number to the largest such
            -- number it received in the DOVIEWCHANGE messages, 
            -- changes its status to normal, and informs the
            -- other replicas of the completion of the view change
            -- by sending STARTVIEW v, l, n, k messages to
            -- the other replicas, where l is the new log, n is the
            -- op-number, and k is the commit-number.
            newest <- pure $ selectLatest $ M.elems receivedMap
            vn <- pure $ (\(DoViewChange vn ol newestNormal on cn rn) -> vn) newest
            ol <- pure $ (\(DoViewChange vn ol newestNormal on cn rn) -> ol) newest
            newestNormal <- pure $ (\(DoViewChange vn ol newestNormal on cn rn) -> newestNormal) newest
            on <- pure $ (\(DoViewChange vn ol newestNormal on cn rn) -> on) newest
            cn <- pure $ (\(DoViewChange vn ol newestNormal on cn rn) -> cn) newest
            rn <- pure $ (\(DoViewChange vn ol newestNormal on cn rn) -> rn) newest
            newestCommit <- pure $ foldl (\a (DoViewChange _ _ _ _ cn _) -> if a > cn then a else cn) 0 (M.elems receivedMap)
            rc <- pure $ rc { opLog = ol }
            rc <- pure $ applyOperations rc newestCommit
            newReplica <- pure $ Primary $ rc{ viewNumber = vn }
            msgs <- pure $ map (\c -> SentMessage c $ StartView vn ol on cn) nonPrimaries
            (msgs, newReplica)
    where isNew = Nothing == (M.lookup sender receivedMap)
          shouldExecute = isNew && rcd == 1
          currentPrimary = getNewPrimary rc (viewNumber rc)
          newPrimary = getNewPrimary rc vn
          isNextPrimary = newPrimary == currentPrimary
          nonPrimaries = filter (/= currentPrimary) (configuration rc)


startRecovery :: Replica -> Event -> ([Event], Replica)
-- The recovering replica, i, sends a RECOVERY i, x
-- message to all other replicas, where x is a nonce.
startRecovery r m = (msgs, recovering)
 where this = replicaNumber $ core r -- Transform in outer shell, generate and store nonce for the replica
       msgs = map (\c -> SentMessage c (Recovery this this)) (configuration $ core r)
       recd = ceiling $ ((fromIntegral $ length $ configuration (core r)) - 1) / 2
       recovering = AwaitingRecoveryResponses (core r) recd Nothing M.empty (viewNumber $ core r)

applyStartView :: Replica -> Event -> ([Event], Replica)
-- If there are non-committed operations in the log, they
-- send a PREPAREOK v, n, ii message to the primary;
-- here n is the op-number. Then they execute all operations
-- known to be committed that they haven’t
-- executed previously, advance their commit-number,
-- and update the information in their client-table
applyStartView r e@(ReceivedMessage _ (StartView vn ol on cn)) = do
    rc <- pure $ core r
    rc <- pure $ rc{ viewNumber = vn, opLog = ol, opNumber = on }
    rc <- pure $ applyOperations rc cn
    ([], Backup rc)

getNewPrimary :: ReplicaCore -> ViewNumber -> Config
getNewPrimary rc vn = (configuration rc) !! primaryIndex
    where primaryIndex = fromIntegral $ vn `mod` (fromIntegral $ length $ configuration rc)


applyStartViewChange :: Replica -> Event -> ([Event], Replica)
applyStartViewChange r@(AwaitingStartViewChanges rc rcd receivedMap lastNorm) m@(ReceivedMessage sender mm@(StartViewChange vn rn)) = case shouldExecute of
    False -> case isNew of
        False -> ([], r) -- Is duplicate. Ignore.
        True -> ([], r{ required = rcd - 1, viewChanges = M.insert sender mm receivedMap })
    True -> case isNextPrimary of -- *** Unsure about what to do here. Should I distinguish between next primary and others or just bulldoze everything and treat all as AwaitingStartView?
        False -> do
            msg <- pure $ DoViewChange (viewNumber rc) (opLog rc) lastNorm (opNumber rc) (commitNumber rc) (replicaNumber rc)
            ([SentMessage newPrimary msg], AwaitingStartView rc)
        True -> do
            msg <- pure $ DoViewChange (viewNumber rc) (opLog rc) lastNorm (opNumber rc) (commitNumber rc) (replicaNumber rc)
            ([SentMessage newPrimary msg], AwaitingStartView rc)
    where isNew = Nothing == (M.lookup sender receivedMap)
          shouldExecute = isNew && rcd == 1
          currentPrimary = getNewPrimary rc (viewNumber rc)
          newPrimary = getNewPrimary rc vn
          isNextPrimary = newPrimary == currentPrimary

respondToRecovery :: Replica -> Event -> ([Event], Replica)
respondToRecovery r m@(ReceivedMessage sender (Recovery _ x)) = ([msg], r)
    where rc = core r
          msg = case isPrimary r of
            True -> SentMessage sender $ RecoveryResponse (viewNumber rc) x (Just $ opLog rc) (Just $ opNumber rc) (Just $ commitNumber rc) (replicaNumber rc)
            False -> SentMessage sender $ RecoveryResponse (viewNumber rc) x Nothing Nothing Nothing (replicaNumber rc)

applyRecoveryResponse :: Replica -> Event -> ([Event], Replica)
applyRecoveryResponse r@(AwaitingRecoveryResponses rc rcd pvn receivedMap ln) m@(ReceivedMessage sender mm@(Recovery _ _)) = case shouldExecute of
    False -> case isNew of
        False -> ([], r)
        True -> ([], r{ required = rcd - 1, viewChanges = M.insert sender mm receivedMap })
    where isNew = Nothing == (M.lookup sender receivedMap)
          contactedPrimary = pvn /= Nothing
          shouldExecute = isNew && rcd == 1 && contactedPrimary

commit :: Replica -> Event -> ([Event], Replica)
-- Then, after it has executed all earlier operations
-- (those assigned smaller op-numbers), the primary
-- executes the operation by making an up-call to the
-- service code, and increments its commit-number
commit r@(AwaitingPrepareOKs rc p pm) m@(ReceivedMessage cfg (PrepareOK vn on rn)) = do
    rc <- pure $ rc{ commitNumber = on }
    operation@(Operation cr@(ClientRequest _ cId _) op) <- pure $ (opLog rc) !! (fromIntegral on)
    (rc, res) <- pure $ apply rc operation
    -- Then it sends a REPLY v, s, xi message to the
    -- client; here v is the view-number, s is the number
    -- the client provided in the request, and x is the result
    -- of the up-call.
    msg <- pure $ SentMessage (ToClient cId) $ Reply vn rn res
    -- The primary also updates the client’s
    -- entry in the client-table to contain the result
    rc <- pure $ rc{ clientTable = M.insert cId (cr, res) (clientTable rc)}
    ([msg], Primary rc)

commitTimeout :: Replica -> Event -> ([Event], Replica)
-- However, if the primary does
-- not receive a new client request in a timely way, it
-- instead informs the backups of the latest commit by
-- sending them a COMMIT v, ki message, where k
-- is commit-number (note that in this case commitnumber = op-number).
commitTimeout r@(Primary rc) SendCommitTimeoutReached = (msgs, r)
    where primaryIdx = primary rc
          currentPrimary = getNewPrimary rc (viewNumber rc)
          nonPrimaries = filter (/= currentPrimary) (configuration rc)
          msgs = map (\c -> SentMessage c $ Commit (viewNumber rc) (commitNumber rc)) nonPrimaries

testRecent :: ReplicaCore -> ClientRequest -> (RequestStatus, Maybe Response)
testRecent rep cr@(ClientRequest o c r)  = case latest of
        Nothing -> (GT, Nothing)
        Just (ClientRequest oo oc or, mRes) -> case compare or r of
            LT -> (LT, Just mRes)
            EQ -> (EQ, Just mRes)
    where latest = M.lookup c (clientTable rep)

pushOp :: ReplicaCore -> Operation -> ReplicaCore
pushOp rep op = rep { opLog = (opLog rep) ++ [op], opNumber = (opNumber rep) + 1}

apply :: ReplicaCore -> Operation -> (ReplicaCore, Response)
apply rc (Operation _ (Get k)) = (rc, Got k $ M.lookup k (store rc))
apply rc (Operation _ (Set k v)) = (newRC, SetOK)
    where newRC = rc { store = M.insert k v (store rc) }

updateClientTable :: ReplicaCore -> ClientID -> ClientRequest -> Response -> ReplicaCore
updateClientTable rc cid cr res = rc{clientTable = newCT}
    where newCT = M.insert cid (cr, res) (clientTable rc)

mostRecent :: ReplicaCore -> ClientID -> Maybe RequestNumber
mostRecent rep cId = case latest of
        Nothing -> Nothing
        Just (req, _) -> Just $ getReq req
    where latest = M.lookup cId (clientTable rep)
          getReq = (\(ClientRequest _ _ r) -> r) 



-- ALTERNATE ATTEMPT: 
-- Write a "runner" for the machine in IO that repeatedly attempts to read from/write to
-- input and output queues. Write separate sources/consumers that do similarly, kick off or process events

run :: Replica -> IO ()
run r = do
    i <- newTChanIO
    o <- newTChanIO
    cTP <- connectCommitTimeoutProducer i o
    runReplica r i o

runReplica :: Replica -> TChan Event -> TChan Event -> IO ()
runReplica r i o = do
    ev <- atomically $ readTChan i
    (res, newRep) <- pure $ step r ev
    mapM (\e -> atomically $ writeTChan o e) res
    runReplica newRep i o

-- Figure out some way to keep track of producers, consumers. Clear them when necessary
data Producer = CommitTimeoutProducer ThreadId deriving (Show, Eq)
connectCommitTimeoutProducer :: TChan Event -> TChan Event -> IO (Producer)
connectCommitTimeoutProducer _i _o = do
        i <- atomically $ cloneTChan _i
        tId <- forkIO $ atomically $ writeTChan _o SendCommitTimeoutReached
        go i _o tId
    where go i o tid = do
            evt <- atomically $ readTChan i
            case evt of
                (ReceivedMessage _ (Commit _ _)) -> do
                    killThread tid
                    tId <- forkIO $ atomically $ writeTChan o SendCommitTimeoutReached
                    go i o tId
                _ -> go i o tid


-- data ReplicaState = ReplicaState { replica :: Replica, input :: TQueue Event, output :: TQueue Event }
-- data ReplicaRunner m a = StateT (m ReplicaState)

-- newReplicaRunner :: Replica -> IO (ReplicaRunner m a)
-- newReplicaRunner r = do
--     i <- newTQueueIO
--     o <- newTQueueIO
--     s <- pure $ ReplicaState r i o
--     return $ return s


-- I need to
-- Fire timeouts when appropriate (and reset after certain messages)
-- "Read" messages from the network or some surrounding thing
-- "Send" subset of messages to the network or some surrounding thing

-- Does it make sense to define a single monad that implements all of the functionality?
-- How do I implement that functionality in a nearly-pure way? (Esp. timeouts?)

-- machine: ReplicaState -> Event -> ([Event], ReplicaState)
-- timeoutWrapper: Event -> IO (ReplicaState -> Event -> ([Event], Replica))
-- { timerId ThreadId, }

{-


 st -> ExernalMsg -> ([Response], st)

 ==>>

 (st , timerst ) -> (ExternalMsg `Either` TimerMsg) -> ([Response], st , timerst )

-}

-- data Time 
-- data TimeReg
-- data  StepperT m a where 
 

-- data TimeoutState m = TimeoutState { stepper :: Stepper m, thread :: ThreadId }

-- data TimeoutStepper m = StateT (TimeoutState m) (Stepper m)

-- listenForTimeout :: (MonadIO m) => Stepper m -> Int -> Event -> TimeoutStepper m
-- listenForTimeout s t e = do
--     tId <- forkIO undefined
--     -- TODO: important timeout business
--     return $ TimeoutState s tId
    



-- -- type MTimeout = StateT

-- class (Monad m) => MonadTimeout m where
--     registerTimeout :: Int -> (TimerId -> m ()) -> m (TimerId)
--     resetTimeout :: TimerId -> m Bool 



-- -- instance MonadTimeout IO where
-- --     setTimeout time s = do
-- --         tId <- forkIO $ threadDelay time >> return () -- TODO
-- --         return s
-- --     resetTimeout = undefined


-- -- newtype Stepper m = Stepper { runStepper :: (Replica -> Event -> m ([Event], Replica)) }

-- -- listenForCommitTimeouts :: (MonadTimeout m) => Stepper m -> Stepper m
-- -- listenForCommitTimeouts s = do
-- --     tId <- setTimeout 100 SendCommitTimeoutReached
-- --     tOut <- undefined
-- --     -- TODO: Pretend right code exists here
-- --     return $ \r e -> do
        
-- --         runStepper s >>= ()
-- --     where \r e -> step 







-- QUESTIONS
-- How to deal with timeouts, context, etc? Should that be on the outside? Should I increase the set of "messages" to include ones fired by local imperative shell re: timeouts, etc?

