using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.OData.Edm.Csdl;
using MongoDB.Driver;
using Newtonsoft.Json;
using REP_AF_SCORING_PROACTIVO.Model;
using System.Configuration;
using System.Reflection.Metadata;
using static System.Reflection.Metadata.BlobBuilder;
using Azure;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Bson;
using REP_AF_SCORING_PROACTIVO_NUEVO.Model;
using NSubstitute.Core;
using System.Text;
using Microsoft.Azure.Documents.Spatial;
using System.Threading.Tasks;
using Microsoft.VisualBasic;
using System.Runtime.CompilerServices;

namespace REP_AF_SCORING_PROACTIVO
{
    public class Proactivo
    {

        public static List<Utility> utilidad = new List<Utility>();

        //Consulta en el Storage el .csv de los clientes Antiguos
        [FunctionName("Proactivo")]
        public static async Task Run([BlobTrigger("output/proactivo/{name}", Connection = "BlobConnecctionScoring")] Stream myBlob, string name, ILogger log)
        {

            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            //Valida el archivo con la extension necesaria.
            if (name.Contains("csv"))
            {

                //AGREGAR variable idcarga_input
                string idCarga_input =await GetInputByEstatus();

                if (!string.IsNullOrEmpty(idCarga_input))
                {

                    await GetBlobs(myBlob, name, idCarga_input);

                    if (utilidad.Count >= 4)
                    {
                        bool control = true;

                        foreach (var item in utilidad)
                        {
                            if (!item.estadocopia)
                            {
                                control = false;
                            } 

                        }

                        if (control)
                        {
                            //ACTUALIZAR ESTADO DE LA CARGA
                            MongoClient cli = new MongoClient(Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                            IMongoDatabase database = cli.GetDatabase("pladik");
                            var collection = database.GetCollection<BsonDocument>("sco_cargahistoricoinputs");

                            //filtrar por _id
                            var filter = Builders<BsonDocument>.Filter.Eq("Estado", 0);
                            var update = Builders<BsonDocument>.Update.Set("Estado", 1);
                            collection.UpdateOne(filter, update);
                        }

                    }

                }
                
            }

        }

        

        private static async Task<string>  GetInputByEstatus()
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

                Console.WriteLine("HA BORRRADO "+name);

                return true;

            }
            else
            {
                return false;
            }
            
        }

        //Metodo base
        public static async Task GetBlobs(Stream myBlob, string name, string idCarga_input)
        {

            Utility respuestainsert = new Utility();

            Console.WriteLine(name);

            if (name.Contains("csv"))
            {

                var res =await InsertData(new StreamReader(myBlob), name, idCarga_input);
             
                respuestainsert.archivo = name;
                respuestainsert.estadocopia = res;             
            }

            if ( !string.IsNullOrEmpty(respuestainsert.archivo))
            {
                utilidad.Add(respuestainsert);
            }

        }

        public static async Task<bool> InsertData(StreamReader streamReader, string blobName, string idCarga_input)
        {

            try
            {
                MongoClient client = new MongoClient(Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                var collection = new List<ScoProactivoNuevo>();
                var collectionAntiguo = new List<ScoProactivoAntiguo>();
                string[] header = { };
                string Line;
                int count = 0;
                IMongoDatabase database = client.GetDatabase("pladik");
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
                    
                    if (collection.Count>0)
                    {
                        IMongoCollection<ScoProactivoNuevo> collectionNue = database.GetCollection<ScoProactivoNuevo>("sco_proactivonuevos");
                        await collectionNue.InsertManyAsync(collection);
                        
                    }
                    return Copy(blobName);

                }
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
                    return Copy(blobName);
                }
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
                            proactivo.Producto = "CREDITO";
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
                    return Copy(blobName);
                }
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
                    return Copy(blobName);
                }

                return false;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
    }

}
