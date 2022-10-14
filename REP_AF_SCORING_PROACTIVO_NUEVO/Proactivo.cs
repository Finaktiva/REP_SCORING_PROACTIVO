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

namespace REP_AF_SCORING_PROACTIVO
{
    public class Proactivo
    {
        //Consulta en el Storage el .csv de los clientes Antiguos
        [FunctionName("Proactivo")]
        public void Run([BlobTrigger("output/proactivo/{name}", Connection = "BlobConnecctionScoring")] Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            //Valida el archivo con la extension necesaria.
            if (name.Contains("csv"))
            {
                //verificar integracion de endpoint
                //agregar variable idcarga_input

                GetBlobs(myBlob, name);

            }
        }

        //Mover de una carpeta a otra
        public bool Copy(string name)
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
                blobClient.Delete();               
            }
            return true;

        }

        //Metodo base
        public void GetBlobs(Stream myBlob, string name)
        {

            Console.WriteLine(name);

            if (name.Contains("csv"))
            {

                var res = InsertData(new StreamReader(myBlob), name);

            }

        }

        public bool InsertData(StreamReader streamReader, string blobName)
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

                            //agregar id
                            //proactivoNuevo.Id_carga_input = ;

                            Console.WriteLine(Line);
                            collection.Add(proactivoNuevo);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }
                    IMongoCollection<ScoProactivoNuevo> collectionNue = database.GetCollection<ScoProactivoNuevo>("sco_proactivonuevo");
                    collectionNue.InsertMany(collection);

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

                            //agregar id
                            //proactivoNuevo.Id_carga_input = ;

                            Console.WriteLine(Line);
                            collection.Add(proactivoNuevo);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }
                    IMongoCollection<ScoProactivoNuevo> collectionNue = database.GetCollection<ScoProactivoNuevo>("sco_proactivonuevo");
                    collectionNue.InsertMany(collection);
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

                            //agregar id
                            //proactivoNuevo.Id_carga_input = ;

                            Console.WriteLine(Line);
                            collectionAntiguo.Add(proactivo);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }
                    IMongoCollection<ScoProactivoAntiguo> collectionNue = database.GetCollection<ScoProactivoAntiguo>("sco_proactivoantiguo");
                    collectionNue.InsertMany(collectionAntiguo);
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

                            //agregar id
                            //proactivoNuevo.Id_carga_input = ;

                            Console.WriteLine(Line);
                            collectionAntiguo.Add(proactivo);
                        }
                        else
                        {
                            header = Line.Split(";");
                        }
                        count++;
                    }
                    IMongoCollection<ScoProactivoAntiguo> collectionNue = database.GetCollection<ScoProactivoAntiguo>("sco_proactivoantiguo");
                    collectionNue.InsertMany(collectionAntiguo);
                }

                var resCopy = Copy(blobName);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
    }
}
