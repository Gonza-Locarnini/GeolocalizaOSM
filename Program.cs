using APIRest_Geo.Models;
using Nominatim.API.Geocoders;
using Nominatim.API.Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;

namespace GeolocalizaOSM
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "ApiGeo", durable: false, exclusive: false, autoDelete: false, arguments: null);
                channel.BasicQos(0, 1, false);
                var consumer = new EventingBasicConsumer(channel);
                channel.BasicConsume(queue: "ApiGeo", autoAck: false, consumer: consumer);

                consumer.Received += (model, ea) =>
                {
                    string response = null;

                    var body = ea.Body.ToArray();
                    var props = ea.BasicProperties;
                    var replyProps = channel.CreateBasicProperties();
                    replyProps.CorrelationId = props.CorrelationId;

                    try
                    {
                        var jsonModel = Encoding.UTF8.GetString(ea.Body.ToArray());
                        var objGeo = JsonSerializer.Deserialize(jsonModel, typeof(Geo));
                        if (objGeo is Geo)
                        {
                            var geo = (Geo)objGeo;
                            var x = new ForwardGeocoder();
                            var r = x.Geocode(new ForwardGeocodeRequest
                            {
                                queryString = geo.Numero.ToString() + " " + geo.Calle + ", " + geo.Ciudad + " " + geo.CodigoPostal + ", " + geo.Provincia + ", " + geo.Pais,
                                BreakdownAddressElements = true,
                                ShowExtraTags = true,
                                ShowAlternativeNames = true,
                                ShowGeoJSON = true
                            });
                            r.Wait();
                            geo.Latitud = r.Result[0].Latitude;
                            geo.Longitud = r.Result[0].Longitude;

                            response = JsonSerializer.Serialize(geo, typeof(Geo));
                        }
                    }
                    catch (Exception e)
                    {
                        response = "";
                    }
                    finally
                    {
                        var responseBytes = Encoding.UTF8.GetBytes(response);
                        channel.BasicPublish(exchange: "", routingKey: props.ReplyTo, basicProperties: replyProps, body: responseBytes);
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                };
                Console.ReadLine();
            }
        }
    }
}
