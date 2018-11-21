using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Data.SQLite;

namespace SoccetsWithDBClient
{
    class Program
    {
        static void Main(string[] args)
        {
            string dbFileName = "StudentsDB.db";
            DataSet dSet = ReadDataFromFile(dbFileName);
            SoccetsSend(dSet);
            //QueueSend(dSet);
            Console.ReadKey();
        }

        public static void SoccetsSend(DataSet dSet)
        {
            int port = 11000;
            try
            { 
                IPAddress ipAddr = IPAddress.Parse("127.0.0.1");
                IPEndPoint ipEndPoint = new IPEndPoint(ipAddr, port);

                Socket sender = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                sender.Connect(ipEndPoint);
                Console.WriteLine("Сокет соединяется с {0} ", sender.RemoteEndPoint.ToString());
                byte[] byteSet = ToBytes<DataSet>(dSet);
                sender.Send(ToBytes<int>(byteSet.Length));
                sender.Send(byteSet);
                sender.Shutdown(SocketShutdown.Both);
                sender.Close();
                Console.WriteLine("Данные отправлены");
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

        public static void QueueSend(DataSet dSet)
        {
            Console.WriteLine("Нажмите клавишу [enter], чтобы отправить данные" );

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");
                string input = "";
                input = Console.ReadLine();
                var body = ToBytes<DataSet>(dSet);
                channel.BasicPublish(exchange: "logs", routingKey: "", basicProperties: null, body: body);
                Console.WriteLine("Данные отправлены!");
            }
        }

        public static DataSet ReadDataFromFile(string dbFileName)
        {
            SQLiteConnection m_dbConn;
            SQLiteCommand m_sqlCmd;

            m_dbConn = new SQLiteConnection();
            m_sqlCmd = new SQLiteCommand();

            String sqlQuery;
            DataSet dSet = new DataSet();

            m_sqlCmd.Connection = m_dbConn;
            m_dbConn = new SQLiteConnection("Data Source=" + dbFileName + ";Version=3;");
            m_dbConn.Open();
            try
            {
                if (m_dbConn.State != ConnectionState.Open)
                {
                    Console.WriteLine("Откройте соединение с базой данных");
                }
                else
                {
                    sqlQuery = "SELECT * FROM Students";
                    SQLiteDataAdapter adapter = new SQLiteDataAdapter(sqlQuery, m_dbConn);
                    adapter.Fill(dSet);
                }
            }
            catch (SQLiteException ex)
            {
                Console.WriteLine("Error: " + ex.Message);
                return null;
            }

            return dSet;
        }

        static byte[] ToBytes<T>(T parameters)
        {
            MemoryStream stream = new System.IO.MemoryStream();
            System.Runtime.Serialization.IFormatter formatter = new BinaryFormatter();
            formatter.Serialize(stream, parameters);

            byte[] bytes = stream.GetBuffer();

            return bytes;
        }

        static T FromBytes<T>(byte[] byteArrayData)
        {
            T parameters;
            using (MemoryStream stream = new MemoryStream(byteArrayData))
            {
                BinaryFormatter bformatter = new BinaryFormatter();
                parameters = (T)bformatter.Deserialize(stream);
            }
            return parameters;
        }
    }
}
