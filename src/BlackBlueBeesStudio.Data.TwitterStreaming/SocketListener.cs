namespace BlackBlueBeesStudio.Data.TwitterStreaming;

using Microsoft.Extensions.Configuration;

using System.ComponentModel;
using System.Net;
using System.Net.Sockets;

internal class SocketListener
{
    public event EventHandler ClientConnected;
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
                Console.WriteLine($"The server is waiting for connection on {ipAddress}:{this.config.GetValue("Port", 9000)}...");

                // as long as we're not pending a cancellation, let's keep accepting requests 
                TcpClient attachedClient = server.AcceptTcpClient();

                var clientIn = new StreamReader(attachedClient.GetStream());
                _clientOut = new StreamWriter(attachedClient.GetStream())
                {
                    AutoFlush = true
                };

                Console.WriteLine($"New client is connected !");

                this.OnClientConnected();
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

    private void OnClientConnected()
    {
        this.ClientConnected?.Invoke(this, EventArgs.Empty);
    }
}
