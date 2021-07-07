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
        private readonly IConfigurationRoot _configuration;
        public Worker(ILogger<Worker> logger, IConfigurationRoot configuration)
        {
            var kafka_uri = _configuration.GetSection("RPKKafka").GetSection("URI").Value;
            _logger = logger;
            _config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "ElasticLoggingGroup",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var kafka_topic = _configuration.GetSection("RPKKafka").GetSection("topic").Value;
            using (var consumer = new ConsumerBuilder<Ignore, string>(_config).Build())
            {
                consumer.Subscribe(kafka_topic);

                while (!stoppingToken.IsCancellationRequested)
                {
                    var consumerResult = consumer.Consume(stoppingToken);
                    _logger.LogInformation($"Consumed data {consumerResult.Message.Value}");
                }
            }
        }
    }
}
