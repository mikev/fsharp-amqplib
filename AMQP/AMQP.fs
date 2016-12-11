namespace AMQP

    open System.Net
    open System.IO
    open System.Net.Sockets
    open System.Text
    open System.Runtime.Serialization
    open System.Runtime.Serialization.Formatters.Binary
    // TDOD open System.Runtime.Serialization.Json
    open AMQP.Util
    open AMQP.CorePickle
    open AMQP.Frames
    open AMQP.Methods

    [<Sealed>]
    [<AutoSerializable(false)>]
    [<CompiledName("Publisher")>]
    type Publisher<'Msg> (hostList:string, port) =

        let mutable tcpSocket = new TcpClient(AddressFamily.InterNetwork)
        let mutable exchange = "amq.direct"
        let mutable userName = "guest"
        let mutable password = "guest"
        let mutable queue = "foobar"
        let mutable vHost = @"/"
        let mutable immediate = false

        let applyTCPCommand (socket:Socket) (writer:BinaryWriter) f =
            #if DEBUG
            printfn "applyTCPCommand"
            #endif
            f writer
            writer.Flush()
            let replyBuffer = Array.zeroCreate<byte> 2048
            let len = socket.Receive(replyBuffer)
            #if DEBUG
            printfn "Socket received: %d bytes" (len)
            #endif
            replyBuffer

        let applyTCPNoReply (socket:Socket) (writer:BinaryWriter) f =
            #if DEBUG
            printfn "applyTCPNoReply"
            #endif
            f writer
            writer.Flush()

        let sendRawHex (socket:Socket) (writer:BinaryWriter) string =
            #if DEBUG
            printfn "sendRawHex %s" string
            #endif
            AMQP.CorePickle.writeSeq writer string
            writer.Flush()
            let replyBuffer = Array.zeroCreate<byte> 2048
            let len = socket.Receive(replyBuffer)
            #if DEBUG
            printfn "Socket received: %d bytes" (len)
            #endif
            replyBuffer

        let sendRawHexNoReply (socket:Socket) (writer:BinaryWriter) string =
            #if DEBUG
            printfn "sendRawHexNoReply %s" string
            #endif
            AMQP.CorePickle.writeSeq writer string
            writer.Flush()
    
        let newStream (tcpSocket:TcpClient) =
            let ipAddress = AMQP.Util.RoundRobinHost(hostList)
            tcpSocket.Connect(ipAddress, port)
            // disable Nagle's algorithm, for more consistently low latency 
            //tcpSocket.NoDelay <- true
            tcpSocket.ReceiveTimeout <- 120
            tcpSocket.SendTimeout <- 120
            tcpSocket.GetStream()

        let mutable netstream = newStream tcpSocket
        let mutable reader = new BinaryReader(new BufferedStream(netstream))
        let mutable writer = new BinaryWriter(new BufferedStream(netstream))
        let mutable connected = false


        member x.Client = tcpSocket.Client
            
        member x.UserName 
            with get() = userName 
            and set(v) = userName <- v
            
        member x.Password 
            with get() = password 
            and set(v) = password <- v
            
        member x.VHost 
            with get() = vHost 
            and set(v) = vHost <- v
            
        member x.Queue 
            with get() = queue 
            and set(v) = queue <- v
            
        member x.Exchange 
            with get() = exchange 
            and set(v) = exchange <- v       

        member x.Immediate 
            with get() = immediate 
            and set(v) = immediate <- v
            
        member x.Reconnect() =
            tcpSocket <- new TcpClient(AddressFamily.InterNetwork)
            netstream <- newStream tcpSocket
            reader <- new BinaryReader(new BufferedStream(netstream))
            writer <- new BinaryWriter(new BufferedStream(netstream))
            connected <- false
            x.Connect()                

        member x.Connect() =
            let command =
                applyTCPCommand tcpSocket.Client writer

            let commandNoReply =
                #if DEBUG
                printfn "commandNoReply"
                #endif
                applyTCPNoReply tcpSocket.Client writer

            let flush =
                writer.Flush()

            #if DEBUG
            printfn "AMQP.Connect"
            #endif
            if connected then
                failwith "AMQPAlreadyConnected"
                //raise (new InvalidOperationException("mailboxProcessorAlreadyStarted"))
            else
                // TODO
                //if not tcpSocket.Connected then
                //    tcpSocket.Connect( hostName, port )

                command (AMQP.Frames.ProtocolHeader) |> ignore
                command (AMQP.Methods.StartOK userName password) |> ignore
                commandNoReply AMQP.Methods.TuneOK
                command (AMQP.Methods.ConnectionOpen vHost) |> ignore
                command (AMQP.Methods.ChannelOpen) |> ignore
                command (AMQP.Methods.QueueDeclare queue 0) |> ignore
                command (AMQP.Methods.QueueBind queue exchange "" false) |> ignore
                connected <- true    

        member x.Disconnect() =
            let command =
                applyTCPCommand tcpSocket.Client writer

            let commandNoReply =
                #if DEBUG
                printfn "commandNoReply"
                #endif
                applyTCPNoReply tcpSocket.Client writer

            let flush =
                writer.Flush()
            #if DEBUG
            printfn "AMQP.Disconnect"
            #endif
            commandNoReply (AMQP.Methods.ChannelClose) |> ignore
            commandNoReply (AMQP.Methods.ConnectionClose) |> ignore
            connected <- false    

        member x.Post ( (msg:'Msg), ?retryCount0 ) =
            let retryCount = defaultArg retryCount0 0
            if retryCount < 0 then
                ()
            #if DEBUG
            printfn "AMQP.Post"
            #endif
            let formatter = new BinaryFormatter()
            let ms = new MemoryStream(256)
            let bw = new BinaryWriter(ms)
            formatter.Serialize(ms, box msg)
            let bin = ms.ToArray()
            x.PostArray(bin,retryCount)            

// TODO - requires Silverlight 5
//        member x.PostJson (msg:'Msg) =
//            #if DEBUG
//            printfn "AMQP.PostJson"
//            #endif
//            let formatter = new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof<'Msg>)
//            let ms = new MemoryStream(256)
//            let bw = new BinaryWriter(ms)
//            formatter.WriteObject(ms, box msg)
//            let bin = ms.ToArray()
//            x.PostArray(bin)            

        member x.PostArray ( (msg:byte[]), ?retryCount0 ) =
            let retryCount = defaultArg retryCount0 0
            if retryCount < 0 then
                ()
            try
                let commandNoReply =
                    #if DEBUG
                    printfn "commandNoReply"
                    #endif
                    applyTCPNoReply tcpSocket.Client writer

                let flush =
                    writer.Flush()

                #if DEBUG
                printfn "AMQP.PostArray"
                #endif
                if msg.Length > AMQP.Frames.FRAME_MAX then failwith "AMQPMessageSizeExceedsFRAME_MAX"
                if immediate then
                    commandNoReply (AMQP.Methods.BasicPublish exchange "" 0x3)
                else
                    commandNoReply (AMQP.Methods.BasicPublish exchange "" 0x1)
                let len = msg.GetLongLength(0)
                commandNoReply (AMQP.Frames.ContentHeader len)
                commandNoReply (AMQP.Frames.ContentBody msg)
                flush
            with
            | _ ->
                x.Reconnect()
                x.PostArray( msg, (retryCount - 1) )

        member x.PostText (msg:string) =
            #if DEBUG
            printfn "AMQP.PostText"
            #endif
            let payload = Encoding.ASCII.GetBytes(msg)
            x.PostArray(payload)
 
        member x.TxSelect =
            let command =
                applyTCPCommand tcpSocket.Client writer
            command (AMQP.Methods.TxSelect)

        member x.TxCommit =
            let command =
                applyTCPCommand tcpSocket.Client writer
            command (AMQP.Methods.TxCommit)

        member x.TxRollback =
            let command =
                applyTCPCommand tcpSocket.Client writer
            command (AMQP.Methods.TxRollback)

        member x.Receive () : 'Msg =
            let command =
                applyTCPCommand tcpSocket.Client writer
            let reply = command (AMQP.Methods.BasicGet queue false)
            let ms = new MemoryStream(reply)
            let reader = new BinaryReader(ms)
            AMQP.CorePickle.u_payload reader |> ignore
            AMQP.CorePickle.u_payload reader |> ignore
            let bin = AMQP.CorePickle.u_payload reader
            // TODO command (AMQP.Methods.BasicAck) |> ignore
            let ms2 = new MemoryStream(bin)
            let formatter = new BinaryFormatter()
            let res = formatter.Deserialize( ms2 )
            unbox res

        member x.ReceiveText () : string =
            let command =
                applyTCPCommand tcpSocket.Client writer
            let reply = command (AMQP.Methods.BasicGet queue false)
            let ms = new MemoryStream(reply)
            let reader = new BinaryReader(ms)
            AMQP.CorePickle.u_payload reader |> ignore
            AMQP.CorePickle.u_payload reader |> ignore
            let bin = AMQP.CorePickle.u_payload reader
            let decoder = new ASCIIEncoding()
            decoder.GetString(bin)

        member x.ReceiveArray () : byte[] =
            let command =
                applyTCPCommand tcpSocket.Client writer
            let reply = command (AMQP.Methods.BasicGet queue false)
            let ms = new MemoryStream(reply)
            let reader = new BinaryReader(ms)
            AMQP.CorePickle.u_payload reader |> ignore
            AMQP.CorePickle.u_payload reader |> ignore
            AMQP.CorePickle.u_payload reader

// TODO - requires Silverlight 5           
//        member x.ReceiveJson () : 'Msg =
//            let reply = command (AMQP.Methods.BasicGet queue false)
//            let ms = new MemoryStream(reply)
//            let reader = new BinaryReader(ms)
//            AMQP.CorePickle.u_payload reader |> ignore
//            AMQP.CorePickle.u_payload reader |> ignore
//            let bin = AMQP.CorePickle.u_payload reader
//            let ms2 = new MemoryStream(bin)
//            let formatter = new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof<'Msg>) 
//            let res = formatter.ReadObject( ms2 )
//            unbox res

        interface System.IDisposable with
            member x.Dispose() =
                x.Disconnect()
                tcpSocket.Close()

        static member Connect(hostList:string, port, userName, password, vhost, queueName) =     
            let mb = new Publisher<'Msg> ( hostList, port )
            mb.UserName <- userName
            mb.Password <- password
            mb.VHost <- vhost
            mb.Queue <- queueName
            mb.Connect();
            mb