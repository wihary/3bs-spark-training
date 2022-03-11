using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;

namespace BlackBlueBeesStudio.Data.TwitterStreaming;

internal class SocketListener
{
    private readonly IConfiguration config;

    private StreamWriter? _clientOut;

    public SocketListener(IConfiguration config)
    {
        this.config = config;
    }

    public void StartServer()
    {
        var worker = new BackgroundWorker();
        worker.WorkerReportsProgress = true;
        worker.WorkerSupportsCancellation = true;

        worker.ProgressChanged += (s, args) =>
        {
            Console.WriteLine(args.UserState);
        };

        worker.DoWork += (s, args) =>
        {
            // startup the server on localhost 
            var ipAddress = IPAddress.Parse(this.config.GetValue("Hostname", "localhost"));
            TcpListener server = new TcpListener(ipAddress, this.config.GetValue("Port", 9000));
            server.Start();

            while (!worker.CancellationPending)
            {
                Console.WriteLine($"The server is waiting on {ipAddress}:{this.config.GetValue("Port", 9000)}...");

                // as long as we're not pending a cancellation, let's keep accepting requests 
                TcpClient attachedClient = server.AcceptTcpClient();

                StreamReader clientIn = new StreamReader(attachedClient.GetStream());
                _clientOut = new StreamWriter(attachedClient.GetStream());
                _clientOut.AutoFlush = true;

                string msg;
                while ((msg = clientIn.ReadLine()) != null)
                {
                    Console.WriteLine("The server received: {0}", msg);
                }
            }
        };

        worker.RunWorkerAsync();
    }

    public async Task SendMessageAsync(string jsonBodyMessage)
    {
        if (_clientOut == null)
            return;

        try
        {
            await _clientOut.WriteAsync(jsonBodyMessage);
        }
        catch (Exception e)
        {
        }
    }
}
