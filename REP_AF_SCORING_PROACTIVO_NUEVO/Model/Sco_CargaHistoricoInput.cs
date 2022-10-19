using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;
using SharpCompress.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace REP_AF_SCORING_PROACTIVO_NUEVO.Model
{
    public class Sco_CargaHistoricoInput
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        public string Nombre { get; set; }

        public string Usuario { get; set; }

        public EstadosInput Estado { get; set; }
        public DateTime? FechaEjecucion { get; set; }

        public enum EstadosInput
        {
            PendientePorProcesar,
            Ejecutado
        }

    }


}
