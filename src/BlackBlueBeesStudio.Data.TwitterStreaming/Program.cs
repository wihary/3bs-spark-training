using BlackBlueBeesStudio.Data.TwitterStreaming;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

CreateHostBuilder(args).Build().Run();

static IHostBuilder CreateHostBuilder(string[] args) =>
    Host.CreateDefaultBuilder(args)
        .ConfigureAppConfiguration(app =>
        {
            app.AddJsonFile("appsettings.json");
        })
        .ConfigureServices((hostContext, services) =>
        {
            services.AddTransient<SocketListener>();
            services.AddHostedService<TwitterStream>();
        });