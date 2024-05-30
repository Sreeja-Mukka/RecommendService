package models

import play.api.libs.json._
case class Book(
                 bno: Int,
                 bname: String,
                 bauthor: String,
                 bsummary: String,
                 bpubyear: Int,
                 brating: Int,
                 bgenre: String
               )

object Book {
  implicit val bookFormat: Format[Book] = Json.format[Book]
}