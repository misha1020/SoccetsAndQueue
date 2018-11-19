using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;


namespace SoccetsWithDBClient
{
    class Program
    {
        static void Main(string[] args)
        {
            //QueueRec();
            SoccetsRec();
        }

        public static void SoccetsRec()
        {
            int port = 11000;
            try
            {
                byte[] key = new byte[8];
                byte[] IV = new byte[8];
                byte[] bytes = new byte[1000000];


                IPHostEntry ipHost = Dns.GetHostEntry("localhost");
                IPAddress ipAddr = ipHost.AddressList[0];
                IPEndPoint ipEndPoint = new IPEndPoint(ipAddr, port);

                Socket sender = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                sender.Connect(ipEndPoint);


                Console.WriteLine("Сокет соединяется с {0} ", sender.RemoteEndPoint.ToString());
                byte[] msg = Encoding.UTF8.GetBytes(" ");
                sender.Send(msg);
                
                //sender.Receive(key);
                //sender.Receive(IV);

                //bytes = DecryptTextFromMemory(bytes, bytesRec, key, IV);
                int bytesRec = sender.Receive(bytes);

                var dt = BytesToDataSet(bytes);
                Print(dt);

                sender.Shutdown(SocketShutdown.Both);
                sender.Close();
            }

            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                Console.ReadLine();
            }
        }

        public static void QueueRec()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                var queueName = channel.QueueDeclare().QueueName;
                channel.QueueBind(queue: queueName,
                                  exchange: "logs",
                                  routingKey: "");

                Console.WriteLine("Ожидание");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var dt = BytesToDataSet(body);
                    Print(dt);
                };
                channel.BasicConsume(queue: queueName,
                                autoAck: true,
                                consumer: consumer);

                Console.WriteLine("Нажмите клавишу [enter], чтобы выйти");
                Console.ReadLine();
            }
        }

        static byte[] DataSetToBytes(DataSet dataSet)
        {
            MemoryStream stream = new System.IO.MemoryStream();
            System.Runtime.Serialization.IFormatter formatter = new BinaryFormatter();
            formatter.Serialize(stream, dataSet);

            byte[] bytes = stream.GetBuffer();

            return bytes;
        }

        static DataSet BytesToDataSet(byte[] byteArrayData)
        {
            DataSet ds;
            using (MemoryStream stream = new MemoryStream(byteArrayData))
            {
                BinaryFormatter bformatter = new BinaryFormatter();
                ds = (DataSet)bformatter.Deserialize(stream);
            }
            return ds;
        }

        static void Print(DataSet finalSet)
        {
            foreach (DataTable dTable in finalSet.Tables)
            {
                Console.WriteLine(dTable.TableName);
                foreach (DataColumn col in dTable.Columns)
                    Console.Write("\t{0}", col.ColumnName);
                Console.WriteLine();
                foreach (DataRow row in dTable.Rows)
                {
                    var cells = row.ItemArray;
                    foreach (object cell in cells)
                        Console.Write("\t{0}", cell);
                    Console.WriteLine();
                }
            }
        }
    }
}
