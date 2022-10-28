using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace REP_AF_SCORING_PROACTIVO_NUEVO.Model
{
    public class ScoInactivoFinanciero
    {
        public ScoInactivoFinanciero()
        {
            FechaConsulta = DateTime.Now.Date;
        }

        [BsonElement("NumeroIdentificacion")]
        public string NumeroIdentificacion { get; set; }

        [BsonElement("Riesgo")]
        public string Riesgo { get; set; }

        [BsonElement("MacroSector")]
        public string MacroSector { get; set; }

        [BsonElement("Producto")]
        public string Producto { get; set; }

        [BsonElement("Sector")]
        public string Sector { get; set; }

        [BsonElement("Act_economica")]
        public string Act_economica { get; set; }

        [BsonElement("Cartera")]
        public string Cartera { get; set; }

        [BsonElement("Activo_Cte")]
        public string Activo_Cte { get; set; }

        [BsonElement("Cartera_AnioAnterior")]
        public string Cartera_AnioAnterior { get; set; }

        [BsonElement("Inventario")]
        public string Inventario { get; set; }

        [BsonElement("Inventario_AnioAnterior")]
        public string Inventario_AnioAnterior { get; set; }

        [BsonElement("Pasivo_Cte")]
        public string Pasivo_Cte { get; set; }

        [BsonElement("Obligaciones_Financieras")]
        public string Obligaciones_Financieras { get; set; }

        [BsonElement("Proveedores")]
        public string Proveedores { get; set; }

        [BsonElement("Proveedores_AnioAnterior")]
        public string Proveedores_AnioAnterior { get; set; }

        [BsonElement("Costos")]
        public string Costos { get; set; }

        [BsonElement("Utilidad_Operacional")]
        public string Utilidad_Operacional { get; set; }

        [BsonElement("Gastos_no_Operativos")]
        public string Gastos_no_Operativos { get; set; }

        [BsonElement("Utilidad_Neta")]
        public string Utilidad_Neta { get; set; }

        [BsonElement("Ebitda")]
        public string Ebitda { get; set; }

        [BsonElement("Total_de_activos")]
        public string Total_de_activos { get; set; }

        [BsonElement("Total_pasivo")]
        public string Total_pasivo { get; set; }

        [BsonElement("Total_patrimonio")]
        public string Total_patrimonio { get; set; }

        [BsonElement("Ingresos")]
        public string Ingresos { get; set; }

        [BsonElement("FechaConsulta")]
        [DisplayFormat(DataFormatString = "{0:dd-MM-yyyy}")]
        public DateTime FechaConsulta { get; set; }

        [BsonElement("Id_carga_inactivo")]
        public string Id_carga_inactivo { get; set; }
    }
}
