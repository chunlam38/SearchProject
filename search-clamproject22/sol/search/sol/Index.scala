package search.sol

import org.xml.sax.SAXParseException
import search.src.FileIO.{printDocumentFile, printTitleFile, printWordsFile}
import scala.xml.Node
import scala.xml.NodeSeq
import scala.util.matching.Regex
import search.src.StopWords.isStopWord
import search.src.PorterStemmer.stem
import java.io.{FileNotFoundException, IOException}
import scala.collection.mutable.HashMap
import scala.math.{pow, sqrt}

/**
  * Provides an XML indexer, produces files for a querier
  * @param inputFile - the filename of the XML wiki to be indexed
  * @param titleFile - the filename of the txt title file indexer will write to
  * @param documentFile  - the filename of the txt document file indexer will write to
  * @param wordsFile - the filename of the txt words file indexer will write to
  */
class Index(val inputFile: String, val titleFile: String, val documentFile: String, val wordsFile: String) {
  val wordsToDocumentFrequencies = new HashMap[String, HashMap[Int, Double]]
  val idsToMaxFreqs = new HashMap[Int, Double] // id -> max value
  var idsList = List[Int]()
  val idsToTitles = new HashMap[Int, String]
  val titleToIds = new HashMap[String, Int] // used to make idsList
  val links = new HashMap[Int, Set[Int]] // doc id to doc id of links
  val pageRankCurr = new HashMap[Int, Double]
  val pageRankPrev = new HashMap[Int, Double]
  val idToLinkWeight = new HashMap[Int, HashMap[Int, Double]]

  /**
    * Populates wordsToDocumentFrequencies and idsToMaxFreqs hashmaps for a single page
    * @param doc - text of a page we are processing
    * @param id - id of document we are processing
    */
  def pageProcessor(doc: String, id: Int): Unit = {
    val tempMap = scala.collection.mutable.Map[String, Int]()
    val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")
    val matchIterator = regex.findAllMatchIn(doc)
    val matchList = matchIterator.toList.map(aMatch => aMatch.matched)
    for (item <- matchList) {
      // for words
      val itemRegex = new Regex(""""[^\W_]+'[^\W_]+|[^\W_]+"""") // 3 or 4 ""?
      if (itemRegex.findAllMatchIn(item).equals(item)) { // item is a word
        if (isStopWord(item).equals(false)) { // not a stop word
          val stemmed = stem(item); // what data structure to store data or directly feed?
          wordsToDocumentFrequencies.get(stemmed) match {
            case Some(idToFreq) => idToFreq.get(id) match {
              case None =>
                wordsToDocumentFrequencies(stemmed).put(id, 1.0)
                tempMap.put(stemmed, 1)
              case _ =>
                wordsToDocumentFrequencies(stemmed)(id) = wordsToDocumentFrequencies(stemmed)(id) + 1.0
                tempMap(stemmed) = tempMap(stemmed) + 1
            }
            case None =>
              val createWordVal = new HashMap[Int, Double]
              createWordVal.put(id, 1.0)
              wordsToDocumentFrequencies.put(stemmed, createWordVal)
              tempMap.put(stemmed, 1)
          }
        }
      } else {
        val linkArray = item.replaceAll("""[\[\]]""", "")
          .replaceAll(""".*\|""", "").toLowerCase().split("\\W") // prepares link
        for (item <- linkArray) {
          if (!isStopWord(item)) {
            val stemmed = stem(item)
            wordsToDocumentFrequencies.get(stemmed) match {
              case Some(idToFreq) => idToFreq.get(id) match {
                case Some(value) =>
                  wordsToDocumentFrequencies(stemmed)(id) = wordsToDocumentFrequencies(stemmed)(id) + 1.0
                  tempMap(stemmed) = tempMap(stemmed) + 1
                case None =>
                  wordsToDocumentFrequencies(stemmed).put(id, 1.0)
                  tempMap.put(stemmed, 1)
              }
              case None =>
                val createLinkVal = new HashMap[Int, Double]
                createLinkVal.put(id, 1.0)
                wordsToDocumentFrequencies.put(stemmed, createLinkVal)
                tempMap.put(stemmed, 1)
            }
          }
        }
      }
    }
    this.idsToMaxFreqs.put(id, tempMap.valuesIterator.max)
  }

  /**
    * Reads the XML file, processes the pages, populates idsList, idsToTitles,
    * and titleToIds, and calculates weight of pages
    */
  def fileReader(): Unit = {
    // convert XML into NodeSeq (Nodes represent pages)
    val mainNode: Node = xml.XML.loadFile(inputFile)
    val pageSeq: NodeSeq = mainNode \ "page"
    for (page <- pageSeq) {
      val docText = (page \ "text").text
      val title = (page \ "title").text
      val allText = docText + title //not sure exactly how to add title to text
      pageProcessor(allText, (page \ "id").text.trim().toInt)
      this.idsList = (page \ "id").text.trim().toInt :: this.idsList
      this.idsToTitles.put((page \ "id").text.trim().toInt, (page \ "title").text.trim())
      this.titleToIds.put((page \ "title").text.trim(), (page \ "id").text.trim().toInt)
    }
    for (page <- pageSeq) {
      this.links += ((page \ "id").text.trim().toInt ->
        linkDocIds((page \ "text").text))
    }
    pageRank()
  }

  /**
    * Returns a list of integers that are all the id of docs linked to a doc
    * @param doc - text from a single doc
    * @return - Returns a list of integers that are all the id of docs linked to a doc
    */
  def linkDocIds(doc: String): Set[Int] = {
    val linkRegex = new Regex("""\[\[(.*?)\]\]""")
    val linkMatchIterator = linkRegex.findAllMatchIn(doc)
    val stringList = linkMatchIterator.toList.map { aMatch => aMatch.matched }
    var modifiedStringList = List[String]()
    for (string <- stringList) {
      modifiedStringList = ((string.replaceAll("""[\[\]]""", ""))
        .replaceAll("""\|.*""", "")) :: modifiedStringList
    }
    var idsList = Set[Int]()
    for (string <- modifiedStringList) {
      if (this.titleToIds.contains(string)) {
        idsList += this.titleToIds(string)
      }
    }
    idsList
  }

  /**
    * Calculates the weight of a page with another page and populates idToLinkWeight and
    * initializes pageRankCurr and pageRankPrev and performs pageRank() method
    */
  def weightCalculation() {
    val numPages = idsToTitles.size
    //iterating over j
    for ((jPage, value) <- this.links) {
      this.pageRankCurr.put(jPage, 1 / numPages.toDouble)
      this.pageRankPrev.put(jPage, 0)
      if (value.isEmpty) { // if not linked (set of doc IDs is empty)
        for ((keyka, valuea) <- this.links) {
          if (jPage != keyka) {
            this.idToLinkWeight.get(keyka) match {
              case Some(smallMapa) => smallMapa.put(jPage,
                (.15 / numPages.toDouble) + 0.85 * (1 / (numPages.toDouble - 1)))
              case None =>
                val idToWeight = new HashMap[Int, Double]()
                idToWeight.put(jPage, (.15 / numPages.toDouble) + (0.85) * (1 / (numPages.toDouble - 1)))
                idToLinkWeight.put(keyka, idToWeight)
            }
          }
        }
      } else {
        for ((kPage, value) <- this.links) {
          if (jPage != kPage) {
            if (value.contains(kPage)) {
              this.idToLinkWeight.get(kPage) match {
                case Some(smallMap) => smallMap.put(jPage,
                  (.15 / numPages.toDouble) + 0.85 * (1 / this.links(jPage).size))
                case None =>
                  val idToWeight = new HashMap[Int, Double]()
                  idToWeight.put(jPage, (.15 / numPages.toDouble) + 0.85 * (1 / this.links(jPage).size))
                  idToLinkWeight.put(kPage, idToWeight)
              }
            } else {
              this.idToLinkWeight.get(jPage) match {
                case Some(smallMap) => smallMap.put(jPage,
                  .15 / numPages.toDouble)
                case None =>
                  val idToWeight = new HashMap[Int, Double]()
                  idToWeight.put(jPage,.15 / numPages.toDouble)
                  idToLinkWeight.put(kPage, idToWeight)
              }
            }
          }
        }
      }
    }
  }

  /**
    * Calculates the pageRank of pages
    */
  def pageRank() {
    var euclidDist = sqrt(1 / this.idsToTitles.size.toDouble)
    var dist = 0.0
    while (euclidDist > 0.0001) {
      weightCalculation()
      for ((jPage, value) <- this.pageRankCurr) {
        this.pageRankPrev.get(jPage) match {
          case None =>
            System.out.println("no value in pagerankprev")
          case Some(x) =>
            this.pageRankPrev.put(jPage, value)
        }
      }
      //val some(x) = hashmap.get(key)
      for ((key, value) <- this.pageRankCurr) {
        this.pageRankCurr.get(key) match {
          case None =>
            System.out.println("nsdflksdjf")
          case Some(x) =>
            var currRank = 0.0
            //sum over all Wjk values for a given page
            this.idToLinkWeight.get(key) match {
              case None =>
                for ((id, score) <- this.pageRankPrev){
                  currRank = currRank + (score * this.pageRankPrev(id))
                }
              case Some(x) =>
                for ((doc, weight) <- this.idToLinkWeight(key)) {
                    currRank = currRank + (weight * this.pageRankPrev(doc))
                }
                this.pageRankCurr(key) = currRank
                dist = pow(currRank - this.pageRankPrev(key), 2)
            }
        }
      }
        euclidDist = sqrt(dist)
      }
    }
}

object Index {
  def main(args: Array[String]) { // (wikiFile, titleFile, documentFile, wordsFile)
    try {
      val page = new Index(args(0), args(1), args(2), args(3))
      page.fileReader()
      printTitleFile(args(1), page.idsToTitles)
      printDocumentFile(args(2), page.idsToMaxFreqs, page.pageRankCurr)
      printWordsFile(args(3), page.wordsToDocumentFrequencies)
    } catch {
      case a: FileNotFoundException => println("File Not Found")
      case b: IOException => println("Some IO Error")
      case c: SAXParseException => println("Error While Parsing Line")
        System.exit(0)
    }
  }
}

