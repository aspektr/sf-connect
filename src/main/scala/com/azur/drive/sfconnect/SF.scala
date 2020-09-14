package com.azur.drive.sfconnect

import sttp.client._
import sttp.client.circe._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.parse
import io.circe.{Decoder, Json, ParsingFailure}
import sttp.client.HttpURLConnectionBackend
import scala.language.postfixOps
import cats.syntax.either._//https://github.com/circe/circe/issues/756#issuecomment-343840355

/**
 * for creating connection to salesforce api
 * @param url
 * @param username
 * @param password
 * @param gType
 * @param clientId
 * @param clientSecret
 */
final case class SF(url:String = "https://login.salesforce.com/services/oauth2/token",
                      username: String,
                      password: String,
                      gType: String = "password",
                      clientId: String,
                      clientSecret: String) {


  private case class MyTokenResponse(access_token: String, instance_url: String, id: String,
                             token_type: String, issued_at: String, signature: String)


  private implicit val tokenResponseDecoder: Decoder[MyTokenResponse] = deriveDecoder[MyTokenResponse]
  private implicit val myBackend = HttpURLConnectionBackend()

  private val tokenRequest = basicRequest
    .post(uri"$url?username=$username&password=$password&grant_type=$gType&client_id=$clientId&client_secret=$clientSecret")
    .auth
    .basic(clientId, clientSecret)
    .header("accept","application/json")
  private val authResponse = tokenRequest.response(asJson[MyTokenResponse]).send()
  val accessToken = authResponse.body.map(_.access_token)

  private def request(url: String): Either[String, String] =  accessToken match {
    case Right(token) => basicRequest.get(uri"$url").auth.bearer(token).send().body
    case Left(e) => Left(e.getMessage)
  }


  private def toJson(resRequest: Either[String, String]): Either[ParsingFailure, Json] = resRequest match {
    case Right(body) => parse(body)
      //this is realy bat shit to pass throwable!!!! when Left occurs the program will crash
    case Left(e) => Left(ParsingFailure(message = e, throw new Exception(e)))
  }

  private def getObjSchema(response: Either[ParsingFailure, Json]): Either[ParsingFailure, Map[String, String]] =
    for {
      listKeys <- response.map(_\\"fields").map(_ flatMap(_ \\ "name"))
      listValues <- response.map(_\\"fields").map(_ flatMap(_ \\ "type"))
    } yield listKeys.map(_.toString.replace(""""""", "")) zip
        listValues.map(_.toString.replace(""""""", "")) toMap

  /**
   * Completely describes the individual metadata at all levels for the specified object.
   * For example, this can be used to retrieve the fields, URLs, and child relationships for the Account object.
   * @param obj specified object (Account, Opportunity etc)
   * @param sfUrl endpoint
   * @param vAPI API version
   * @return Map(fieldName->fieldType)
   */
  def describe(obj: String,
               sfUrl: String = "https://finsys.my.salesforce.com/",
               vAPI: String = "v49.0"): Either[ParsingFailure, Map[String, String]] = {
    val url = s"${sfUrl}services/data/$vAPI/sobjects/$obj/describe/"
    (request _ andThen toJson andThen getObjSchema) (url)
  }


  /**
   * make a soql query
   * @param obj specified object (Account, Opportunity etc)
   * @param predicate condition to filter
   * @return soql query for salesforce
   */
  def soql(obj:String, predicate: String = ""): Either[ParsingFailure, String] = for {
      schemaObj <- this.describe(obj)
      keys      = schemaObj.keys.mkString(",").replace(""""""", "")
      soql      = s"select $keys from $obj" + predicate
  } yield soql

}
