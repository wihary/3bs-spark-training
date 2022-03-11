namespace BlackBlueBeesStudio.Data.TwitterStreaming;

using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

using Tweetinvi;
using Tweetinvi.Models;
using Tweetinvi.Parameters.V2;
using Tweetinvi.Streaming.V2;

internal class TwitterStream : IHostedService
{
    private readonly IConfiguration config;
    private readonly SocketListener socketListener;
    private IFilteredStreamV2? _stream;
    public TwitterClient? TwitterClient { get; set; }

    public TwitterStream(IConfiguration config, SocketListener socketListener)
    {
        this.config = config;
        this.socketListener = socketListener;
        this.socketListener.StartServer();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await this.BuildTwitterClientAsync();

        if(this.TwitterClient != null)
        {
            _stream = this.TwitterClient.StreamsV2.CreateFilteredStream();
            await this.TwitterClient.StreamsV2.AddRulesToFilteredStreamAsync(new FilteredStreamRuleConfig("#france"));

            _stream.TweetReceived += (sender, eventReceived) =>
            {
                Console.WriteLine(eventReceived.Json);
                this.socketListener.SendMessageAsync(eventReceived.Json);
            };

            await _stream.StartAsync();
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _stream?.StopStream();
        return Task.CompletedTask;
    }

    private async Task BuildTwitterClientAsync()
    {
        var consumerOnlyCredentials = new ConsumerOnlyCredentials(this.config.GetValue("CONSUMER_KEY", string.Empty), this.config.GetValue("CONSUMER_SECRET", string.Empty));
        var appClientWithoutBearer = new TwitterClient(consumerOnlyCredentials);

        var bearerToken = await appClientWithoutBearer.Auth.CreateBearerTokenAsync();
        var appCredentials = new ConsumerOnlyCredentials("CONSUMER_KEY", "CONSUMER_SECRET")
        {
            BearerToken = bearerToken
        };

        this.TwitterClient = new TwitterClient(appCredentials);
    }
}
