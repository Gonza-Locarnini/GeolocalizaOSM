using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace APIRest_Geo.Models
{
    public class Geo
    {
        public enum eEstados { Procesando, Terminado };
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        [StringLength(100)]
        public string Calle { get; set; }
        public int Numero { get; set; }
        [StringLength(100)]
        public string Ciudad { get; set; }
        public int CodigoPostal { get; set; }
        [StringLength(100)]
        public string Provincia { get; set; }
        [StringLength(100)]
        public string Pais { get; set; }
        [Required]
        public double Latitud { get; set; }
        public double Longitud { get; set; }
        public eEstados Estado { get; set; }

        public Geo()
        {
            Calle = "";
            Numero = 0;
            Ciudad = "";
            CodigoPostal = 0;
            Provincia = "";
            Pais = "";
            Latitud = 0;
            Longitud = 0;
            Estado = eEstados.Procesando;
        }
    }
}
