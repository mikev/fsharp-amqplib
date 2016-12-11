
#load "Util.fs"
#load "Pickle.fs"
#load "Frames.fs"
#load "Methods.fs"
#load "AMQP.fs"
open AMQP

open System

 
// Example:
let test = AMQP.Util.Scramble ['1' .. '9'] |> Seq.toList
// Output:
// val test : char list = ['3'; '6'; '7'; '5'; '4'; '8'; '2'; '1'; '9']

let tcpHostName = "tdbrabbit1.np.ev1.yellowpages.com"
let tcpHostList = "tdbrabbit1.np.ev1.yellowpages.com,tdbrabbit6.np.ev1.yellowpages.com"

let HostList hosts =
    let GetHostAddresses list =
        try
            Some(System.Net.Dns.GetHostAddresses( list ))
        with
        |_  -> None
    seq { for host in hosts do
            let ipList = GetHostAddresses( host )
            match ipList with
            | Some(result) -> yield! result
            | None -> () }

let hosts =
    tcpHostList.Split [|',';' ';'\t'|]
    |> Seq.ofArray

let choice = AMQP.Util.Scramble (HostList hosts) |> Seq.head

let gchoice =
    tcpHostList.Split [|',';' ';'\t'|]
    |> Seq.ofArray
    |> HostList
    |> AMQP.Util.Scramble
    |> Seq.head  


let hostList = "tdbrabbit1.np.ev1.yellowpages.com,tdbrabbit6.np.ev1.yellowpages.com"
let tcpPort = 5672
let amqp = new AMQP.Publisher<Map<string,string>> ( hostList, tcpPort )

amqp.Queue <- "foobar"
amqp.Connect()

amqp.PostText "Hello World CCC"
let text = amqp.ReceiveText()

amqp.Disconnect()
amqp.Reconnect()

let addresses = Map.ofList [ "Jeff", "123 MainStreet, Redmond, WA 98052";
                             "Fred", "987 Pine Road, Phila., PA 19116";
                             "Mary", "PO Box 112233, Palo Alto, CA 94301" ]

amqp.Post (addresses, 2)
let replyAddress = amqp.Receive()

amqp.TxSelect
amqp.PostText "HelloWorld3"
amqp.TxCommit