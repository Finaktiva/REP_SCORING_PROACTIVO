using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace REP_AF_SCORING_PROACTIVO_NUEVO.Model
{
    public class ScoInactivo
    {
        public ScoInactivo()
        {
            FechaConsulta = DateTime.Now.Date;
            Variables = new Variables();
        }


        public string Id_carga_inactivo { get; set; }
        public string NumeroIdentificacion { get; set; }
        public string Razon_Social { get; set; }
        public string Producto { get; set; }
        public string Riesgo { get; set; }
        public string Riesgo_Etiquetado { get; set; }

        [DisplayFormat(DataFormatString = "{0:dd-MM-yyyy}")]
        public DateTime FechaConsulta { get; set; }
        public Variables Variables { get; set; }

    }
    public class Variables
    {
        public Variables()
        {
            Headers = new List<string>();
            Values = new List<string>();

        }
        public List<string> Headers { get; set; }
        public List<string> Values { get; set; }
    }
}


