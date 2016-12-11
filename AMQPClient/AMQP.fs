namespace AMQPClient

    open System.Net
    open System.IO
    open System.Net.Sockets
    open System.Text

    [<Sealed>]
    [<AutoSerializable(false)>]
    [<CompiledName("AMQP")>]
    type AMQP (initial, hostName:string, port, ?cancellationToken) =

        let cancellationToken = defaultArg cancellationToken Async.DefaultCancellationToken
        let tcpSocket = new TcpClient(AddressFamily.InterNetwork)
        let mutable exchange = "amq.direct"
        let mutable userName = "guest"
        let mutable password = "guest"
        let mutable queue = "foobar"
        let mutable vHost = @"/"

        let sendCommand (socket:Socket) string =
            let memStream = new MemoryStream(1024)
            use bw = new BinaryWriter(memStream)
            AMQPClient.CorePickle.writeSeq bw string
            let sendBuffer = memStream.ToArray()
            let ret = socket.Send( sendBuffer )
            let replyBuffer = Array.zeroCreate<byte> 2048
            let len = socket.Receive(replyBuffer)
            printfn "Socket received: %d bytes" (len)
            replyBuffer

        let sendTCPCommand (socket:Socket) (writer:BinaryWriter) string =
            AMQPClient.CorePickle.writeSeq writer string
            writer.Flush()
            let replyBuffer = Array.zeroCreate<byte> 2048
            let len = socket.Receive(replyBuffer)
            printfn "Socket received: %d bytes" (len)
            replyBuffer

        let applyTCPCommand (socket:Socket) (writer:BinaryWriter) f =
            f writer
            writer.Flush()
            let replyBuffer = Array.zeroCreate<byte> 2048
            let len = socket.Receive(replyBuffer)
            printfn "Socket received: %d bytes" (len)
            replyBuffer

        let applyTCPNoReply (socket:Socket) (writer:BinaryWriter) f =
            printfn "applyTCPNoReply"
            f writer
            writer.Flush()

        let sendTCPBinary (socket:Socket) (writer:BinaryWriter) (b:byte[]) =
            AMQPClient.CorePickle.p_bytes b writer
            writer.Flush()
            let replyBuffer = Array.zeroCreate<byte> 2048
            let len = socket.Receive(replyBuffer)
            printfn "Socket received: %d bytes" (len)
            replyBuffer

        let sendTCPNoReply (socket:Socket) (writer:BinaryWriter) string =
            AMQPClient.CorePickle.writeSeq writer string
            writer.Flush()
    
        let newStream =
            tcpSocket.Connect(hostName, port)
            // disable Nagle's algorithm, for more consistently low latency 
            //tcpSocket.NoDelay <- true
            tcpSocket.ReceiveTimeout <- 120
            tcpSocket.SendTimeout <- 120
            tcpSocket.GetStream()

        let netstream = newStream
        let reader = new BinaryReader(new BufferedStream(netstream))
        let writer = new BinaryWriter(new BufferedStream(netstream))
        let mutable connected = false
        let errorEvent = new Event<System.Exception>()

        let command =
            applyTCPCommand tcpSocket.Client writer

        let commandNoReply =
            printfn "commandNoReply"
            applyTCPNoReply tcpSocket.Client writer

        let flush =
            writer.Flush()

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
            
        [<CLIEvent>]
        member x.Error = errorEvent.Publish

        member x.Connect() =
            if connected then
                failwith "AMQPAlreadyConnected"
                //raise (new InvalidOperationException("mailboxProcessorAlreadyStarted"))
            else
                command (AMQPClient.Frame.ProtocolHeader) |> ignore
                command (AMQPClient.Frame.StartOK userName password) |> ignore
                commandNoReply AMQPClient.Frame.TuneOK
                command (AMQPClient.Frame.ConnectionOpen vHost) |> ignore
                command (AMQPClient.Frame.ChannelOpen) |> ignore
                command (AMQPClient.Frame.QueueDeclare queue 0) |> ignore
                command (AMQPClient.Frame.QueueBind queue exchange "" false) |> ignore

                connected <- true

                // Protect the execution and send errors to the event
                let p = async { try 
                                    do! initial x 
                                with err -> 
                                    errorEvent.Trigger err }

                Async.Start(computation=p, cancellationToken=cancellationToken)

        member x.Post (msg:byte[]) =
            printfn "x.Post"
            commandNoReply (AMQPClient.Frame.BasicPublish exchange "" 0x1)
            let len = msg.GetLongLength(0)
            commandNoReply (AMQPClient.Frame.ContentHeader len)
            commandNoReply (AMQPClient.Frame.ContentBody msg)
            flush

        member x.PostText (msg:string) =
            printfn "x.PostText"
            let payload = Encoding.ASCII.GetBytes(msg)
            x.Post(payload)

        member x.TxSelect =
            command (AMQPClient.Frame.TxSelect)

        member x.TxCommit =
            command (AMQPClient.Frame.TxCommit)

        member x.TxRollback =
            command (AMQPClient.Frame.TxRollback)

        member x.Receive () =
            let reply = command (AMQPClient.Frame.BasicGet queue false)
            let ms = new MemoryStream(reply)
            let reader = new BinaryReader(ms)
            let r2 = AMQPClient.CorePickle.u_payload reader
            let r3 = AMQPClient.CorePickle.u_payload reader
            AMQPClient.CorePickle.u_payload reader
           
        interface System.IDisposable with
            member x.Dispose() = tcpSocket.Close()

        static member Connect(hostName:string, port, userName, password, vhost, queueName) = 
            let initial = (fun inbox ->
                               let rec loop n =
                                    async { printfn "initial %d" n }
                               loop 0
                            )            
            let mb = new AMQP( initial, hostName, port )
            mb.UserName <- userName
            mb.Password <- password
            mb.VHost <- vhost
            mb.Queue <- queueName
            mb.Connect();
            mb