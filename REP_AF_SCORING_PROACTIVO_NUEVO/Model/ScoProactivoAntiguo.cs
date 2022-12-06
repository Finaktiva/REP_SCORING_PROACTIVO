using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace REP_AF_SCORING_PROACTIVO.Model
{
    public class ScoProactivoAntiguo
    {
        public ScoProactivoAntiguo()
        {
            FechaConsulta = DateTime.Now.Date;
        }

        [BsonElement("NumeroIdentificacion")]
        public string NumeroIdentificacion { get; set; }

        [BsonElement("Razon_Social")]
        public string Razon_Social { get; set; }

        [BsonElement("IRS")]
        public string IRS { get; set; }

        [BsonElement("DinamicaEconomica")]
        public string DinamicaEconomica { get; set; }

        [BsonElement("Producto")]
        public string Producto { get; set; }

        [BsonElement("Riesgo")]
        public string Riesgo { get; set; }

        [BsonElement("Riesgo_Etiquetado")]
        public string Riesgo_Etiquetado { get; set; }

        [BsonElement("FechaConsulta")]
        [DisplayFormat(DataFormatString = "{0:dd-MM-yyyy}")]
        public DateTime FechaConsulta { get; set; }

        [BsonElement("Id_carga_input")]
        public string Id_carga_input { get; set; }
    }
}
