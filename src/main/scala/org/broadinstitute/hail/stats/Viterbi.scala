package org.broadinstitute.hail.stats

import org.broadinstitute.hail.utils.MultiArray2

object Viterbi {

  def argmax(a: Array[Double]): Int = {
    val max = a.max
    a.indexOf(max)
  }

  def apply[T](data: Array[T],
    transitions: MultiArray2[Double],
    starts: Array[Double],
    emissions: Array[(T) => Double]): (Double, Array[Int]) = {
    require(transitions.n1 == transitions.n2, "transition matrix is not square")
    require((transitions.n1 == starts.length) && (emissions.length == transitions.n1),
      s"""inconsistent HMM inputs:
          |  Transition matrix: [${transitions.n1} x ${transitions.n2}]
          |  Initialization vector: [${starts.length} x 1]
          |  Emission vector: [${emissions.length} x 1]
          |
          |  Inputs should be [k x k], [k x 1], and [k x 1],
          |    where k is the number of hidden states.
      """.stripMargin)

    val nStates = transitions.n1

    val states = (0 until nStates).toArray

    val P = MultiArray2.fill[Double](data.length, nStates)(0)
    val prevStates = MultiArray2.fill[Int](data.length, nStates)(-1)

    for (j <- 0 until nStates) {
      P.update(0, j, starts(j) * emissions(j)(data(0)))
    }

    for (i <- 1 until data.length) {
      for (j <- states) {
        val probs = states.map(prevState => P(i - 1, prevState) * transitions(prevState, j))
        val prev = argmax(probs)
        prevStates.update(i, j, prev)
        P.update(i, j, probs(prev) * emissions(j)(data(i)))
      }
    }

    val endValues = states.map(st => P(data.length - 1, st))
    val backtrack = Array.fill[Int](data.length)(0)

    var prev = argmax(endValues)
    val maxP = endValues(prev)

    backtrack(data.length - 1) = prev

    for (i <- data.length - 2 until 0 by -1) {
      backtrack(i) = prevStates(i + 1, prev)
      prev = prevStates(i, prev)
    }

    (maxP, backtrack)

  }
}
