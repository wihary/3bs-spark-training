namespace BlackBlueBeesStudio.Data.TwitterStreaming;

using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

using Tweetinvi;
using Tweetinvi.Models;
using Tweetinvi.Models.V2;
using Tweetinvi.Parameters.V2;
using Tweetinvi.Streaming.V2;

internal class TwitterStream : IHostedService
{
    private readonly IConfiguration config;
    private readonly SocketListener socketListener;
    private IFilteredStreamV2? _stream;
    private bool _isStreamRunning = false;
    private long _tweetCount;

    public TwitterClient? TwitterClient { get; set; }

    public TwitterStream(IConfiguration config, SocketListener socketListener)
    {
        this.config = config;
        this.socketListener = socketListener;
        this.socketListener.StartServer();
        this.socketListener.ClientConnected += async (sender, e) => await this.SocketListener_ClientConnectedAsync(sender, e);
    }
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await this.BuildTwitterClientAsync();
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

    private async Task SocketListener_ClientConnectedAsync(object? sender, EventArgs e)
    {
        if (this.TwitterClient != null && !this._isStreamRunning)
{
            Console.WriteLine($"Starting stream from Twitter Api v2");
            _stream = this.TwitterClient.StreamsV2.CreateFilteredStream();
            await this.TwitterClient.StreamsV2.AddRulesToFilteredStreamAsync(new FilteredStreamRuleConfig("#france"));

            _stream.TweetReceived += async (sender, eventReceived) =>
            {
                _tweetCount++;
                var hashtags = eventReceived.Tweet.ContextAnnotations != null 
                ? string.Join(",", eventReceived.Tweet.ContextAnnotations.Select(x => x.Domain.Name).Distinct()) 
                : string.Empty;

                Console.WriteLine($"Tweet {_tweetCount}, text = [{eventReceived.Tweet.Text}, hashtags = [{hashtags}]");
                await this.socketListener.SendMessageAsync(eventReceived.Tweet.Text);
            };

            await _stream.StartAsync();
            _isStreamRunning = true;
        }
    }
}
