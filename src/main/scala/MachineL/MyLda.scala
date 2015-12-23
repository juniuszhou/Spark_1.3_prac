package MachineL

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.mutable

object MyLda {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "lda")
    val path = "/home/junius/github/spark/data/mllib/sample_lda_data.txt"
    val file = scala.io.Source.fromFile(path)

    val textRDD: RDD[String] = sc.makeRDD(file.getLines().toArray)

    // Split text into words
    // val tokenizer = new RegexTokenizer("\\s+")

    // map to index and an array of words
    val tokenized =
      textRDD.zipWithIndex().map(strIndex => (strIndex._2, strIndex._1.split(" ")))

    tokenized.cache()

    // Counts words and stored to word and count
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()
    //val fullVocabSize = wordCounts.count()

    //get all vocabulary and sort according to frequency.
    val vocab: Map[String, Int] =
        wordCounts.collect().sortBy(-_._2).map(_._1).zipWithIndex.toMap

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }


    // init lda model and set 3 topic.
    val lda = new LDA().setK(3)

    // change data to Vector for parallel computing. and vector also is LDA 's interface required.
    val documents = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb = Vectors.sparse(vocab.size, indices, values)
      (id, sb)
    }

    // run and get model.
    val ldaModel = lda.run(documents)

    //get top 10 word for each topic
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 5)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }

    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }

    // stop job.
    sc.stop()
  }
}
