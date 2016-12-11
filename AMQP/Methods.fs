namespace AMQP

module Methods =

    open System.IO
    open AMQP.CorePickle
    open AMQP.Frames

    type ConnectionID =
        | Start = 10
        | StartOK = 11
        | Secure = 20
        | SecureOK = 21
        | Tune = 30
        | TuneOK = 31
        | Open = 40
        | OpenOK = 41
        | Close = 50
        | CloseOK = 51

    type ChannelID =
        | Open = 10
        | OpenOK = 11
        | Flow = 20
        | FlowOK = 21
        | Close = 40
        | CloseOK = 41

    type QueueID =
        | Declare = 10
        | DeclareOK = 11
        | Bind = 20
        | BindOK = 21
        | Unbind = 50
        | UnbindOK = 51
        | Purge = 30
        | PurgeOK = 31
        | Delete = 40
        | DeleteOK = 41

    type BasicID =
        | Qos = 10
        | QosOK = 11
        | Consume = 20
        | ConsumeOK = 21
        | Cancel = 30
        | CancelOK = 31
        | Publish = 40
        | PublishOK = 41
        | Return = 50
        | Deliver = 60
        | Get = 70
        | GetOK = 71
        | GetEmpty = 72
        | Ack = 80
        | Reject = 90
        | RecoverAsync = 100
        | Recover = 110
        | RecoverOK = 111

    type TxID =
        | Select = 10
        | SelectOK = 11
        | Commit = 20
        | CommitOK = 21
        | Rollback = 30
        | RollbackOK = 31

    let p_classID (id:ClassID) bw =
        p_short (id.GetHashCode()) bw

    let p_methodID id bw =
        p_short (id.GetHashCode()) bw

    //
    // AMQP Protocol Methods
    //
    // These functions return function values, which take a BinaryWriter as input
    // and output the packet compatible command.
    //

    let StartOK (name:string) (password:string) =
        let StartOKBinary (name:string) (password:string) =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Connection bw // ClassID "connection" 10
            p_methodID ConnectionID.StartOK bw // MethodID "start ok" 11
            p_long 0 bw
            p_shortString "PLAIN" bw
            let len = name.Length + password.Length + 2
            p_long len bw
            p_zeroString name bw
            p_zeroString password bw
            p_shortString "en_US" bw
            ms.ToArray()
        let cmd = StartOKBinary name password
        MethodFrame0 cmd

    let TuneOK : (BinaryWriter -> Unit) =
        let TuneOKBinary =
            let ms = new MemoryStream(16)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Connection bw // ClassID "connection" 10
            p_methodID ConnectionID.TuneOK bw // MethodID "tune ok" 31
            p_short 0 bw // Channel Max
            p_long FRAME_MAX bw // Frame Max
            p_short 0 bw // Heartbeat
            ms.ToArray()
        let cmd = TuneOKBinary
        MethodFrame0 cmd

    let ConnectionOpen path =
        let ConnectionOpenBinary path =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Connection bw // ClassID "connection" 10
            p_methodID ConnectionID.Open bw // MethodID "open" 40
            p_shortString path bw
            p_short 0 bw // Padding
            ms.ToArray()
        let cmd = ConnectionOpenBinary path
        MethodFrame0 cmd     

    let ConnectionClose : (BinaryWriter -> Unit) =
        let ConnectionCloseBinary =
            let ms = new MemoryStream(16)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Connection bw // ClassID "connection" 10
            p_methodID 60 bw // MethodID "close" supposed to be 50 ??
            p_short 200 bw // Reply-Code 200
            p_shortString "200" bw // Reply-Text "200"
            p_short 0 bw // failing classID 0
            p_short 0 bw // failing methodID 0
            ms.ToArray()
        let cmd = ConnectionCloseBinary
        MethodFrame0 cmd      

    let ChannelOpen : (BinaryWriter -> Unit) =
        let ChannelOpenBinary =
            let ms = new MemoryStream(8)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Channel bw // ClassID "channel" 20
            p_methodID ChannelID.Open bw // MethodID "open" 10
            p_byte 0 bw // reserved field
            ms.ToArray()
        let cmd = ChannelOpenBinary
        MethodFrame1 cmd

    let ChannelClose : (BinaryWriter -> Unit) =
        let ChannelCloseBinary =
            let ms = new MemoryStream(16)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Channel bw // ClassID "channel" 20
            p_methodID ChannelID.Close bw // MethodID "close" 40
            p_short 200 bw // Reply-Code 200
            p_shortString "200" bw // Reply-Text "200"
            p_short 0 bw // failing classID 0
            p_short 0 bw // failing methodID 0
            ms.ToArray()
        let cmd = ChannelCloseBinary
        MethodFrame1 cmd

    let TxSelect : (BinaryWriter -> Unit) =
        let TxSelectBinary =
            let ms = new MemoryStream(8)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Tx bw // ClassID "tx" 90
            p_methodID TxID.Select bw // MethodID "select" 10
            ms.ToArray()
        let cmd = TxSelectBinary
        MethodFrame1 cmd

    let TxCommit : (BinaryWriter -> Unit) =
        let TxCommitBinary =
            let ms = new MemoryStream(8)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Tx bw // ClassID "tx" 90
            p_methodID TxID.Commit bw // MethodID "commit" 20
            ms.ToArray()
        let cmd = TxCommitBinary
        MethodFrame1 cmd

    let TxRollback : (BinaryWriter -> Unit) =
        let TxRollbackBinary =
            let ms = new MemoryStream(8)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Tx bw // ClassID "tx" 90
            p_methodID TxID.Rollback bw // MethodID "rollback" 30
            ms.ToArray()
        let cmd = TxRollbackBinary
        MethodFrame1 cmd

    let QueueDeclare name flags =
        let QueueDeclareBinary name =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Queue bw // ClassID "queue" 50
            p_methodID QueueID.Declare bw // MethodID "declare" 10
            p_reserved1 bw // reserved
            p_shortString name bw
            p_byte flags bw // passive-durable-exclusive-autodelete-nowait bits
            p_emptyTable bw
            ms.ToArray()
        let cmd = QueueDeclareBinary name
        MethodFrame1 cmd

    let MirroredQueueDeclare name flags =
        let QueueDeclareBinary name =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Queue bw // ClassID "queue" 50
            p_methodID QueueID.Declare bw // MethodID "declare" 10
            p_reserved1 bw // reserved
            p_shortString name bw
            p_byte flags bw // passive-durable-exclusive-autodelete-nowait bits
            p_long 1 bw // arg field table of length 1
            p_stringfieldvaluepair "x-ha-policy" "all" bw
            ms.ToArray()
        let cmd = QueueDeclareBinary name
        MethodFrame1 cmd

    let QueueBind queueName exchangeName routingKey noWait =
        let QueueBindBinary queueName exchangeName routingKey =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Queue bw // ClassID "queue" 50
            p_methodID QueueID.Bind bw // MethodID "bind" 20
            p_reserved1 bw // reserved
            p_shortString queueName bw
            p_shortString exchangeName bw
            p_shortString routingKey bw
            if noWait then
                p_byte 1 bw // no-wait bit
            else
                p_byte 0 bw
            p_emptyTable bw
            ms.ToArray()      
        let cmd = QueueBindBinary queueName exchangeName routingKey
        MethodFrame1 cmd

    let bitmask2 a b =
        match (a,b) with
        | false,false -> 0x0
        | true,false -> 0x1
        | false,true -> 0x2
        | true,true -> 0x3

    let BasicPublish exchangeName routingKey flags =
        let BasicPublishBinary exchangeName routingKey =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Basic bw // ClassID "basic" 60
            p_methodID BasicID.Publish bw // MethodID "bind" 40
            p_reserved1 bw // reserved-1
            p_shortString exchangeName bw
            p_shortString routingKey bw
            p_byte flags bw // mandatory (Queue must exist), immediate (Subscriber must exist) bit
            ms.ToArray()
        let cmd = BasicPublishBinary exchangeName routingKey
        MethodFrame1 cmd

    let BasicConsume queueName flags =
        let BasicConsumeBinary queueName =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Basic bw // ClassID "basic" 60
            p_methodID BasicID.Consume bw // MethodID "consume" 20
            p_reserved1 bw // reserved-1
            p_shortString queueName bw
            p_byte 0 bw // consumer tag
            p_byte flags bw // local, ack, exclusive, no-wait bits 0x2
            ms.ToArray()
        let cmd = BasicConsumeBinary queueName
        MethodFrame1 cmd

    let BasicGet queueName noAck =
        let BasicGetBinary queueName =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_classID ClassID.Basic bw // ClassID "basic" 60
            p_methodID BasicID.Get bw // MethodID "get" 70
            p_reserved1 bw // reserved-1
            p_shortString queueName bw
            if noAck then
                p_byte 1 bw // no-ack bit
            else
                p_byte 0 bw
            ms.ToArray()
        let cmd = BasicGetBinary queueName
        MethodFrame1 cmd
