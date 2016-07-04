package org.broadinstitute.hail.stats

import org.broadinstitute.hail.utils.MultiArray2
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test

class HMMSuite extends TestNGSuite {

  @Test def test() {

    /**
      * Took this example from wikipedia:
      * https://en.wikipedia.org/wiki/Viterbi_algorithm
      *
      * states = ('Healthy', 'Fever')
      * observations = ('normal', 'cold', 'dizzy')
      * start_probability = {'Healthy': 0.6, 'Fever': 0.4}
      * transition_probability = {
      *    'Healthy' : {'Healthy': 0.7, 'Fever': 0.3},
      *    'Fever' : {'Healthy': 0.4, 'Fever': 0.6}
      *    }
      * emission_probability = {
      *    'Healthy' : {'normal': 0.5, 'cold': 0.4, 'dizzy': 0.1},
      *    'Fever' : {'normal': 0.1, 'cold': 0.3, 'dizzy': 0.6}
      *    }
      *
      *    Expected result:
      *    The steps of states are Healthy Healthy Fever with highest probability of 0.01512
      **/

    val observed = Array(0,1,2)
    val transitionMatrix = MultiArray2.fill[Double](2, 2)(0)
    transitionMatrix.update(0, 0, 0.7)
    transitionMatrix.update(0, 1, 0.3)
    transitionMatrix.update(1, 0, 0.4)
    transitionMatrix.update(1, 1, 0.6)

    val startProbability = Array(0.6, 0.4)
    val emissionProbability = Array(
      (i: Int) => (i: @unchecked) match {
        case 0 =>   0.5
        case 1 =>   0.4
        case 2 =>   0.1
      },
      (i: Int) => (i: @unchecked) match {
        case 0 =>   0.1
        case 1 =>   0.3
        case 2 =>   0.6
      }
    )

    val (maxp, result) = Viterbi(observed, transitionMatrix, startProbability, emissionProbability)
    assert(result.toIndexedSeq == IndexedSeq(0, 0, 1))
    assert(maxp == 0.01512)
  }
}
