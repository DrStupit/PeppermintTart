using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Public;
using Kafka.Public.Loggers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

namespace ElasticLoggingService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly ConsumerConfig _config;
        private readonly IConfiguration _configuration;
        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _config = new ConsumerConfig
            {
                BootstrapServers = configuration.GetSection("RPKKafka").GetSection("URI").Value,
                GroupId = "eslogginggroup",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using (var consumer = new ConsumerBuilder<Ignore, string>(_config).Build())
            {
                consumer.Subscribe(_configuration.GetSection("RPKKafka").GetSection("topic").Value);

                while (!stoppingToken.IsCancellationRequested)
                {
                    var consumerResult = consumer.Consume(stoppingToken);
                    _logger.LogInformation($"Consumed data {consumerResult.Message.Value}");
                }
            }
        }
    }
}
