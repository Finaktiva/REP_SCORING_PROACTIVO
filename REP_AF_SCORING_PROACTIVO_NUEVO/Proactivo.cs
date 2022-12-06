using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using REP_AF_SCORING_PROACTIVO.Model;
using REP_AF_SCORING_PROACTIVO_NUEVO.Model;
using SharpCompress.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Metadata;
using System.Threading.Tasks;

namespace REP_AF_SCORING_PROACTIVO
{
    public class Proactivo
    {

        public static string idCarga_input = "";
        public static string idCarga_inactivo = "";

        [FunctionName("Proactivo")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "processProactivoBlob")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            await BlobScan(log);

            string responseMessage = "This HTTP triggered function executed successfully";

            return new OkObjectResult(responseMessage);
        }


        //Consulta en el Storage el .csv de los clientes Antiguos
        //private static async Task Run([BlobTrigger("output/proactivo/{name}", Connection = "BlobConnecctionScoring")] Stream myBlob, string name, ILogger log)
        private static async Task BlobScan(ILogger log)
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(Environment.GetEnvironmentVariable("BlobConnecctionScoring"));
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(Environment.GetEnvironmentVariable("ContainerName"));

            var blobs = blobContainerClient.GetBlobsAsync(BlobTraits.All, BlobStates.All, "proactivo/");
            await foreach (BlobItem blobItem in blobs)
            {
                var name = blobItem.Name;
                var length = blobItem.Properties.ContentLength;
 
                log.LogInformation($"C# Http Blob trigger function Processed blob\n Name:{name} \n Size: {length} Bytes");

                //Valida el archivo con la extension necesaria.
                if (name.Contains(".csv") || name.Contains(".CSV"))
                {
                    BlobClient blobClient = blobContainerClient.GetBlobClient(name);

                    using (var memoryStream = new MemoryStream())
                    {
                        blobClient.DownloadTo(memoryStream);
                        memoryStream.Position = 0;

                        if (string.IsNullOrEmpty(idCarga_input))
                        {
                            //SE CARGA EL ID DEL HISTORICO INPUT
                            idCarga_input = await GetInputByEstatus();
                        }

                        if (string.IsNullOrEmpty(idCarga_inactivo))
                        {
                            //SE CARGA EL ID DEL HISTORICO INACTIVO
                            idCarga_inactivo = await GetIdHistoricoInactivo();
                        }



                        if (!string.IsNullOrEmpty(idCarga_input) || !string.IsNullOrEmpty(idCarga_inactivo))
                        {

                            //REALIZAMOS LOS INSERT
                            string control = await InsertData(new StreamReader(memoryStream), name, idCarga_input, idCarga_inactivo);


                            if (control == "PROACTIVO")
                            {
                                //ACTUALIZAR ESTADO DE LA CARGA EN HISTORICO INPUT
                                MongoClient cli = new MongoClient(Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                                IMongoDatabase database = cli.GetDatabase("pladik");
                                var collection = database.GetCollection<BsonDocument>("sco_cargahistoricoinputs");

                                //filtrar por _id
                                var filter = Builders<BsonDocument>.Filter.Eq("Estado", 0);
                                var update = Builders<BsonDocument>.Update.Set("Estado", 1);
                                collection.UpdateOne(filter, update);
                            }


                            if (control == "INACTIVO")
                            {
                                //ACTUALIZAR ESTADO DE LA CARGA EN HISTORICO INACTIVO
                                MongoClient cli = new MongoClient(Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                                IMongoDatabase database = cli.GetDatabase("pladik");
                                var collection = database.GetCollection<BsonDocument>("sco_cargahistoricoinactivos");

                                //filtrar por _id
                                var filter = Builders<BsonDocument>.Filter.Eq("Estado", 0);
                                var update = Builders<BsonDocument>.Update.Set("Estado", 1);
                                collection.UpdateOne(filter, update);
                            }

                        }
                    }

                }

            }
        }



        private static async Task<string> GetInputByEstatus()
        {
            MongoClient client = new MongoClient(Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
            IMongoDatabase database = client.GetDatabase("pladik");
            var collection = database.GetCollection<BsonDocument>("sco_cargahistoricoinputs");
            var documents = await collection.FindAsync(new BsonDocument("Estado", 0)).Result.FirstOrDefaultAsync();

            if (documents != null)
            {
                return documents.First().Value.ToString();
            }
            else
            {
                return (string.Empty);
            }
        }

        private static async Task<string> GetIdHistoricoInactivo()
        {
            MongoClient client = new MongoClient(Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
            IMongoDatabase database = client.GetDatabase("pladik");
            var collection = database.GetCollection<BsonDocument>("sco_cargahistoricoinactivos");
            var documents = await collection.FindAsync(new BsonDocument("Estado", 0)).Result.FirstOrDefaultAsync();

            if (documents != null)
            {
                return documents.First().Value.ToString();
            }
            else
            {
                return (string.Empty);
            }
        }

        //Mover de una carpeta a otra
        public static bool Copy(string name)
        {

            string storageConnectionString = Environment.GetEnvironmentVariable("BlobConnecctionScoring");

            string containerName = Environment.GetEnvironmentVariable("ContainerName");
            var blobContainerClient = new BlobContainerClient(storageConnectionString, containerName);
            var blobContainerClientCopy = new BlobContainerClient(storageConnectionString, containerName);

            if (name.Contains("csv"))
            {
                var blobClient = blobContainerClient.GetBlobClient("proactivo/" + name);
                var blobsCopy = blobContainerClientCopy.GetBlobClient($"{Environment.GetEnvironmentVariable("blobCopy")}{name}");
                blobsCopy.StartCopyFromUri(blobClient.Uri);
                var response = blobClient.Delete();

                Console.WriteLine("HA BORRRADO " + name);

                return true;

            }
            else
            {
                return false;
            }

        }

        public static async Task<string> InsertData(StreamReader streamReader, string blobName, string idCarga_input, string idCarga_inactivo)
        {

            try
            {
                MongoClient client = new MongoClient(Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                var collection = new List<ScoProactivoNuevo>();
                var collectionAntiguo = new List<ScoProactivoAntiguo>();
                var collectionInactivo = new List<ScoInactivo>();
                //var collectionInactivo = new List<ScoInactivoFinanciero>();
                string[] header = { };
                string Line;
                int count = 0;
                IMongoDatabase database = client.GetDatabase("pladik");

                //MODELO PROACTIVO NUEVO FINANCIERO
                if (blobName.Contains("Output_Modelo_Financiero"))
                {
                    while ((Line = streamReader.ReadLine()) != null)
                    {
                        if (count > 0)
                        {
                            var list = Line.Split(";");
                            ScoProactivoNuevo proactivoNuevo = new ScoProactivoNuevo();
                            proactivoNuevo.NIT = list[0];
                            proactivoNuevo.Razon_Social = list[1].ToString();
                            proactivoNuevo.Calificacion = list[2].ToString();
                            proactivoNuevo.Producto = "FINANCIERO";
                            proactivoNuevo.riesgo_ss = list[3].ToString();
                            proactivoNuevo.MacroSector = list[4].ToString();
                            proactivoNuevo.Sector = list[5].ToString();
                            proactivoNuevo.Act_economica = list[6].ToString();
                            proactivoNuevo.Cartera = list[7].ToString();
                            proactivoNuevo.Activo_Cte = list[8].ToString();
                            proactivoNuevo.Cartera_AnioAnterior = list[9].ToString();
                            proactivoNuevo.Inventario = list[10].ToString();
                            proactivoNuevo.Inventario_AnioAnterior = list[11].ToString();
                            proactivoNuevo.Pasivo_Cte = list[12].ToString();
                            proactivoNuevo.Obligaciones_Financieras = list[13].ToString();
                            proactivoNuevo.Proveedores = list[14].ToString();
                            proactivoNuevo.Proveedores_AnioAnterior = list[15].ToString();
                            proactivoNuevo.Costos = list[16].ToString();
                            proactivoNuevo.Utilidad_Operacional = list[17].ToString();
                            proactivoNuevo.Gastos_no_Operativos = list[18].ToString();
                            proactivoNuevo.Utilidad_Neta = list[19].ToString();
                            proactivoNuevo.Ebitda = list[20].ToString();
                            proactivoNuevo.Total_de_activos = list[21].ToString();
                            proactivoNuevo.Total_pasivo = list[22].ToString();
                            proactivoNuevo.Total_patrimonio = list[23].ToString();
                            proactivoNuevo.Ingresos = list[24].ToString();
                            proactivoNuevo.FechaConsulta = DateTime.Now;
                            proactivoNuevo.Id_carga_input = idCarga_input;

                            Console.WriteLine(Line);
                            collection.Add(proactivoNuevo);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }

                    if (collection.Count > 0)
                    {
                        IMongoCollection<ScoProactivoNuevo> collectionNue = database.GetCollection<ScoProactivoNuevo>("sco_proactivonuevos");
                        await collectionNue.InsertManyAsync(collection);

                    }
                    bool retornoCopy = Copy(blobName);
                    if (retornoCopy)
                    {
                        return ("PROACTIVO");
                    }
                }

                //MODELO PROACTIVO NUEVO LIBERA
                else if (blobName.Contains("Output_Informacion_libera"))
                {
                    while ((Line = streamReader.ReadLine()) != null)
                    {
                        if (count > 0)
                        {
                            var list = Line.Split(";");
                            ScoProactivoNuevo proactivoNuevo = new ScoProactivoNuevo();
                            proactivoNuevo.NIT = list[0];
                            proactivoNuevo.Razon_Social = list[1].ToString();
                            proactivoNuevo.Calificacion = list[2].ToString();
                            proactivoNuevo.Producto = "LIBERA";
                            proactivoNuevo.FechaConsulta = DateTime.Now;
                            proactivoNuevo.Id_carga_input = idCarga_input;

                            Console.WriteLine(Line);
                            collection.Add(proactivoNuevo);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }

                    if (collection.Count > 0)
                    {
                        IMongoCollection<ScoProactivoNuevo> collectionNue = database.GetCollection<ScoProactivoNuevo>("sco_proactivonuevos");
                        await collectionNue.InsertManyAsync(collection);

                    }
                    bool retornoCopy = Copy(blobName);
                    if (retornoCopy)
                    {
                        return ("PROACTIVO");
                    }
                }

                //MODELO PROACTIVO ANTIGUO CREDITO
                else if (blobName.Contains("Output_Modelo_Credito"))
                {
                    while ((Line = streamReader.ReadLine()) != null)
                    {
                        if (count > 0)
                        {
                            var list = Line.Split(";");
                            ScoProactivoAntiguo proactivo = new ScoProactivoAntiguo();
                            proactivo.NumeroIdentificacion = list[0];
                            proactivo.Razon_Social = list[1].ToString();
                            proactivo.IRS = list[2].ToString();
                            proactivo.DinamicaEconomica = list[3].ToString();
                            proactivo.Producto = "Credito/Confirming";
                            proactivo.Riesgo = list[5].ToString();
                            proactivo.Riesgo_Etiquetado = list[6].ToString();
                            proactivo.FechaConsulta = DateTime.Now;
                            proactivo.Id_carga_input = idCarga_input;

                            Console.WriteLine(Line);
                            collectionAntiguo.Add(proactivo);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }
                    if (collectionAntiguo.Count > 0)
                    {
                        IMongoCollection<ScoProactivoAntiguo> collectionNue = database.GetCollection<ScoProactivoAntiguo>("sco_proactivoantiguos");
                        await collectionNue.InsertManyAsync(collectionAntiguo);

                    }
                    bool retornoCopy = Copy(blobName);
                    if (retornoCopy)
                    {
                        return ("PROACTIVO");
                    }
                }

                //MODELO PROACTIVO ANTIGUO FACTORING
                else if (blobName.Contains("Output_Modelo_Factoring"))
                {
                    while ((Line = streamReader.ReadLine()) != null)
                    {
                        if (count > 0)
                        {
                            var list = Line.Split(";");
                            ScoProactivoAntiguo proactivo = new ScoProactivoAntiguo();
                            proactivo.NumeroIdentificacion = list[0];
                            proactivo.Razon_Social = list[1].ToString();
                            proactivo.IRS = list[2].ToString();
                            proactivo.DinamicaEconomica = list[3].ToString();
                            proactivo.Producto = "FACTORING";
                            proactivo.Riesgo = list[5].ToString();
                            proactivo.Riesgo_Etiquetado = list[6].ToString();
                            proactivo.FechaConsulta = DateTime.Now;
                            proactivo.Id_carga_input = idCarga_input;



                            Console.WriteLine(Line);
                            collectionAntiguo.Add(proactivo);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }
                    if (collectionAntiguo.Count > 0)
                    {
                        IMongoCollection<ScoProactivoAntiguo> collectionNue = database.GetCollection<ScoProactivoAntiguo>("sco_proactivoantiguos");
                        await collectionNue.InsertManyAsync(collectionAntiguo);

                    }
                    bool retornoCopy = Copy(blobName);
                    if (retornoCopy)
                    {
                        return ("PROACTIVO");
                    }
                }

                //MODELO INACTIVO ANTIGUO CREDITO
                else if (blobName.Contains("Output_Flujo_Inactivo_Modelo_Credito") || blobName.Contains("Output_Flujo_Inactivo_Modelo_Factoring"))
                {
                    while ((Line = streamReader.ReadLine()) != null)
                    {
                        if (count > 0)
                        {
                            var list = Line.Split(";");

                            //SE USA LA SIGUIENTE COLECCION YA QUE LOS MODELOS SON IGUALES 
                            ScoInactivo inactivo = new ScoInactivo
                            {
                                NumeroIdentificacion = list[0],
                                Razon_Social = list[1].ToString(),
                                Producto = list[4],
                                Riesgo = list[5].ToString(),
                                Riesgo_Etiquetado = list[6].ToString(),
                                FechaConsulta = DateTime.Now,
                                Id_carga_inactivo = idCarga_inactivo
                            };

                            for (int i = 2; i < (header.Length - 3); i++)
                            {
                                inactivo.Variables.Headers.Add(header[i]);
                                inactivo.Variables.Values.Add(list[i]);
                            }
                            Console.WriteLine(Line);
                            collectionInactivo.Add(inactivo);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }
                    if (collectionInactivo.Count > 0)
                    {
                        IMongoCollection<ScoInactivo> collectionNue = database.GetCollection<ScoInactivo>("sco_inactivos");
                        await collectionNue.InsertManyAsync(collectionInactivo);

                    }
                    bool retornoCopy = Copy(blobName);
                    if (retornoCopy)
                    {
                        return ("INACTIVO");
                    }
                }



                //MODELO INACTIVO ANTIGUO FINANCIERO
                else if (blobName.Contains("Output_Flujo_Inactivo_Modelo_Financiero"))
                {
                    while ((Line = streamReader.ReadLine()) != null)
                    {
                        if (count > 0)
                        {
                            var list = Line.Split(";");

                            ScoInactivo inactivoFinanciero = new ScoInactivo
                            {
                                NumeroIdentificacion = list[0],
                                Producto = "Financiero",
                                Riesgo = list[1].ToString()

                            };
                            for (int i = 2; i < (header.Length); i++)
                            {
                                inactivoFinanciero.Variables.Headers.Add(header[i]);
                                inactivoFinanciero.Variables.Values.Add(list[i]);
                            }
                            inactivoFinanciero.Id_carga_inactivo = idCarga_inactivo;
                            Console.WriteLine(Line);
                            collectionInactivo.Add(inactivoFinanciero);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }

                    if (collectionInactivo.Count > 0)
                    {
                        IMongoCollection<ScoInactivo> collectionNue = database.GetCollection<ScoInactivo>("sco_inactivos");
                        await collectionNue.InsertManyAsync(collectionInactivo);
                    }
                    bool retornoCopy = Copy(blobName);
                    if (retornoCopy)
                    {
                        return ("INACTIVO");
                    }
                }
                else
                {
                    return "";
                }
                return "";
            }
            catch (Exception ex)
            {
                return "";
            }
        }
    }

}
