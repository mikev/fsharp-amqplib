namespace AMQPClient

//
// For details on Pickler theory see:
// Functional PEARL Pickler Combinators paper by Andrew Kennedy
// http://research.microsoft.com/pubs/64036/picklercombinators.pdf
//

module CorePickle =

    open System
    open System.IO
    open System.Net

    type WriterState = System.IO.BinaryWriter
    type ReaderState = System.IO.BinaryReader

    type pickler<'T> = 'T -> WriterState -> unit
    type unpickler<'T> = ReaderState -> 'T

    let p_char (b:char) (st:WriterState) = st.Write(b)

    let p_byte (b:int) (st:WriterState) = st.Write (byte b)
    let u_byte (st:ReaderState) = (int (st.ReadByte()))

    let p_bool (b:bool) (st:WriterState) = p_byte (if b then 1 else 0) st
    let u_bool st = let b = u_byte st in (b = 1) 

    let p_int32 (n:int32) (st:WriterState) =
        st.Write (int32 (IPAddress.HostToNetworkOrder(n)))
    let u_int32 (st:ReaderState) =
        IPAddress.NetworkToHostOrder(st.ReadInt32())

    let p_int c st = p_int32 c st
    let p_int8 (i:sbyte) st = p_int32 (int32 i) st
    let p_uint8 (i:byte) st = p_byte (int i) st

    let p_int16 (i:int16) (st:WriterState) =
        st.Write (int16 (IPAddress.HostToNetworkOrder(int16 i)))
    let u_int16 (st:ReaderState) =
        IPAddress.NetworkToHostOrder(st.ReadInt16())

    let p_uint16 (x:uint16) (st:WriterState) =
         st.Write (uint16 (IPAddress.HostToNetworkOrder(int16 x)))
    let u_uint16 (st:ReaderState) =
        (uint16 (IPAddress.NetworkToHostOrder(st.ReadInt16())))

    let p_uint32 (x:uint32) st = p_int32 (int32 x) st

    let p_int64 (i:int64) (st:WriterState) =
        st.Write (IPAddress.HostToNetworkOrder(int64 i))
    let u_int64 (st:ReaderState) =
        IPAddress.NetworkToHostOrder(st.ReadInt64())

    let p_uint64 (x:uint64) st = p_int64 (int64 x) st

    let p_octet (i:int) st = p_byte i st

    let p_short (i:int) st = p_uint16 (uint16 i) st
    let u_short (st:ReaderState) = u_uint16 st

    let p_long (i:int) st = p_int32 i st
    let u_long st = u_int32 st

    let p_longlong (i:int64) st = p_int64 i st
    let u_longLong st = u_int64 st

    let p_shortString (str:string) (st:WriterState) = st.Write str
    let u_shortString (st:ReaderState) =
        st.ReadString

    let p_zeroString (str:string) (st:WriterState) =
        let chars = str.ToCharArray()
        p_byte 0 st
        st.Write chars

    let p_emptyTable st = p_int32 0 st

    let p_bytes (s:byte[]) (st:WriterState) =
        p_long s.Length st
        st.Write s
    let u_bytes (st:ReaderState) =
        let len = u_long st
        st.ReadBytes(len)

    let u_payload (st:ReaderState) =
        let t = u_byte st
        let chan = u_short st
        let pLen = u_long st
        let payload = st.ReadBytes( pLen )
        let eof = u_byte st
        if (eof <> 0xce) then failwith "Unframed packet"
        payload

    let u_payload2 (st:ReaderState) =
        let t = u_byte st
        let chan = u_short st
        let pLen = u_long st
        let payload = st.ReadBytes( pLen )
        let eof = u_byte st
        if (eof <> 0xce) then failwith "Unframed packet"
        (t, chan, pLen, payload)

    let inline p_tup2 p1 p2 (a,b) (st:WriterState) = (p1 a st : unit); (p2 b st : unit)
    let inline p_tup3 p1 p2 p3 (a,b,c) (st:WriterState) = (p1 a st : unit); (p2 b st : unit); (p3 c st : unit)

    let inline  u_tup2 p1 p2 (st:ReaderState) = let a = p1 st in let b = p2 st in (a,b)
    let inline  u_tup3 p1 p2 p3 (st:ReaderState) =
      let a = p1 st in let b = p2 st in let c = p3 st in (a,b,c)

    let ByteToHex bytes = 
        bytes 
        |> Array.map (fun (x : byte) -> System.String.Format("{0:X2}", x))
        |> String.concat System.String.Empty

    let writeSeq (writer:BinaryWriter) str =

        // map hex ascii to binary
        let hexCharToByte value =
            match value with
            | _ when (value >= '0' && value <= '9') ->
                (byte value - byte '0')
            | _ when (value >= 'a' && value <= 'f') ->
                (byte value - byte 'a' + byte 10)
            | _ when (value >= 'A' && value <= 'F') ->
                (byte value - byte 'A' + byte 10)
            | _ -> byte 0

        // transform elem -> (elem * elem)
        let pairs seq =
            seq
            |> Seq.pairwise
            |> Seq.mapi (fun i x -> if i%2=0 then Some(x) else None)
            |> Seq.choose id

        // transform string to binary
        let stringToByteSeq input =
            input
            |> Seq.map (fun a -> hexCharToByte a)

        // put pairs into binary int
        let pairsToInt (x:seq<byte * byte>) =
            x
            |> Seq.map (fun (a,b) -> ((a <<< 4) ||| b))

        str
        |> stringToByteSeq
        |> pairs
        |> pairsToInt
        |> Seq.iter (fun (a:byte) -> writer.Write a)

module Sample =

    // Sample packets (in Hex format) extracted from WireShark while observing AMQP sessions.
    // These are useful for both unit testing and development.

    let ProtocolHeader = "414d515001010800"
    let StartOK = "01000000000024000a000b0000000005504c41494e0000000c00677565737400677565737405656e5f5553ce"
    let TuneOK = "0100000000000c000a001f0000000200000000ce"
    let ConnectionOpen = "01000000000008000a0028012f0000ce"
    let ChannelOpen = "010001000000050014000a00ce"
    let QueueDeclare = "010001000000120032000a000006666f6f6361720000000000ce"
    let QueueBind = "0100010000001800320014000006666f6f6361720454657374000000000000ce"
    let BasicPublish = "0100010000000d003c0028000004546573740000ce"
    let BasicConsume = "0100010000000f003c0014000006666f6f6361720002ce"
    let BasicGetFull = "01000100000017003c004700000000000000010004546573740000000000ce0200010000001a003c0000000000000000000e90000a746578742f706c61696e02ce0300010000000e48656c6c6f576f726c645f414141ce"
    let ContentHeader = "0200010000001a003c0000000000000000000a90000a746578742f706c61696e02ce"
    let ContentBody = "0300010000000a48656c6c6f576f726c64ce"
    let ChannelClose = "0100010000000e0014002800c80332303000000000ce"
    let ConnectionClose = "0100000000000e000a003c00c80332303000000000ce"


module Frame =

    //
    // AMQP client protocol
    //
    // AMQP Protocol documentation:
    // http://www.amqp.org/resources/download
    //
    
    open System.IO
    open CorePickle

    let CH_01 = 1
    let FRAME_MAX = 0x20000

    type FrameID =
        | Method = 1
        | Header = 2
        | Body = 3
        | HeartBeat = 8
        | End = 0xce

    type ClassID = 
        | Connection  = 10
        | Channel = 20
        | Exchange = 40
        | Queue = 50
        | Basic = 60
        | Tx = 90

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

    let p_methodType bw =
        p_byte (FrameID.Method.GetHashCode()) bw

    let p_headerType bw =
        p_byte (FrameID.Header.GetHashCode()) bw

    let p_bodyType bw =
        p_byte (FrameID.Body.GetHashCode()) bw

    let p_frameEnd bw =
        p_byte (FrameID.End.GetHashCode()) bw

    let p_reserved1 bw =
        p_short 0 bw

    //
    // Frame Header section
    //
    // AMQP begins each command with a "Header Frame"
    // There are several types of headers, including method, content and body.
    // For AMQP Protocol documentation see: http://www.amqp.org/resources/download
    //

    let ProtocolFrame (payload:byte[]) (writer:BinaryWriter) =
        // Define protocol version: 'AMQP0091'
        p_char 'A' writer
        p_char 'M' writer
        p_char 'Q' writer
        p_char 'P' writer
        p_byte 0 writer
        p_byte 0 writer
        p_byte 9 writer
        p_byte 1 writer

    let MethodFrame0 (payload:byte[]) (writer:BinaryWriter) =
        p_methodType writer // Frame type 1
        p_short 0 writer
        p_bytes payload writer
        p_frameEnd writer // Frame end
 
    let MethodFrame1 (payload:byte[]) (writer:BinaryWriter) =
        p_methodType writer // Frame type 1
        p_short CH_01 writer
        p_bytes payload writer
        p_frameEnd writer // Frame end
 
    let HeaderFrame (payload:byte[]) (writer:BinaryWriter) =
        p_headerType writer // Frame type 2
        p_short CH_01 writer
        p_bytes payload writer
        p_frameEnd writer // Frame end

    let BodyFrame (payload:byte[]) (writer:BinaryWriter) =
        p_byte (FrameID.Body.GetHashCode()) writer // Frame type 3
        p_short CH_01 writer
        p_bytes payload writer
        p_frameEnd writer // Frame end

    let ProtocolHeader:(BinaryWriter -> unit) =
        let ProtocolHeaderBinary =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            ms.ToArray()
        let cmd = ProtocolHeaderBinary
        let result = ProtocolFrame cmd
        result
        
    let ContentHeader contentSize =
        let ContentHeaderBinary contentSize =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_short (ClassID.Basic.GetHashCode()) bw // ClassID - must match method frame class id
            p_short 0 bw // weight field - unused, must be zero
            p_longlong contentSize bw // total size of the content body, i.e. sum of body sizes for content body frames
            p_short 0x9000 bw // Property flags
            p_shortString @"text/plain" bw
            p_byte 0x2 bw // Delivery mode bit
            ms.ToArray()
        let cmd = ContentHeaderBinary contentSize
        HeaderFrame cmd

    let ContentBody (payload:byte[]) =
        BodyFrame payload

    //
    // AMQP Protocol Methods
    //
    // These return functions, which take a BinaryWriter as input
    // and create the command.
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
            p_methodID QueueID.Declare bw // MethodID "open" 10
            p_reserved1 bw // reserved
            p_shortString name bw
            p_byte flags bw // passive-durable-exclusive-autodelete-nowait bits
            p_emptyTable bw
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
