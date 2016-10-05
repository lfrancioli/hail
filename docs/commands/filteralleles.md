<div class="cmdhead"></div>

<div class="cmdsubsection">
Filter a user-defined set of alternate alleles for each variant. If all of a variant's alternate alleles are filtered, the variant itself is filtered. The condition expression is evaluated for each alternate allele. It is not evaluated for the reference (i.e. `aIndex` will never be zero).

There are two algorithms implemented to remove an allele from the Genotypes (`--subset` and `--downcode`).
In addition to these two modes, the `--filterAlteredGenotypes` option will set any genotype (and thus would change when removing the allele) that contained the filtered allele to missing.
The example below illustrate the behavior of these two algorithms when filtering allele _1_ in the following example Genotype at a site with 3 alleles (reference and 2 non-reference alleles).

```
GT: 1/2
GQ: 10
AD: 0,50,35

0 | 1000
1 | 1000   10
2 | 1000   0     20
  +-----------------
    0      1     2
```

### Subsetting algorithm

When using the `--subset` option, subsets the AD and PL arrays (i.e. remove entries with filtered allele) and sets GT to the genotype with the minimum likelihood.
Note that if the Genotype changes (like in the example), the PLs are re-normalized so that the most-likely genotype has a PL of _0_.
The qualitative interpretation of subsetting is a belief that the alternate is not-real and we want to discard any probability mass associated with the alternate.

The subsetting algorithm would produce the following:
```
GT: 1/1
GQ: 980
AD: 0,50

0 | 980
1 | 980    0
  +-----------
     0      1
```

|Part|Description|Action|
|---|---|---|
|GT|the hard call|The most-likely genotype based on the PLs ignoring the filtered allele(s)|
|AD|allele depth|the filtered alleles' columns are eliminated, e.g. filtering alleles 1 and 2 transforms `[25,5,10,20]` to `[25,20]`|
|DP|number of informative reads|no change|
|PL|Phred-likelihoods for each allele pair|Subsets the PLs to those associated with remaining alleles (and normalize)|
|GQ|genotype quality|increasing-sort PL and take `PL[1] - PL[0]`|

### Downcoding algorithm

When using the `--downcode` option, the PL array convert occurences of the filtered allele to the reference (e.g. 1 -> 0 in our example).
It takes minimums where there are multiple likelihoods for a single genotype. The genotype is then set accordingly.
Similarly, the depth for the filtered allele in the AD field is added to that of the reference.
If an allele is filtered, this algorithm acts similarly to [`splitmulti`](#splitmulti).

The downcoding algorithm would produce the following:
```
GT: 0/1
GQ: 10
AD: 35,50

0 | 20
1 | 0    10
  +-----------
    0    1
```

|Part|Description|Action|
|---|---|---|
|GT|the hard call|downcode the filtered alleles to reference|
|AD|allele depth|the filtered alleles' columns are eliminated and the value is added to the reference, e.g. filtering alleles 1 and 2 transforms `[25,5,10,20]` to `[40,20]`|
|DP|number of informative reads|no change|
|PL|Phred-likelihoods for each allele pair|downcode the filtered alleles and take the minimum of the likelihoods for each genotype|
|GQ|genotype quality|increasing-sort PL and take `PL[1] - PL[0]`|
</div>

<div class="synopsis"></div>

<div class="options"></div>

<div class="cmdsubsection">
### Expression Variables
The following table describes the variables in the scope of `COND_EXPR`

| Name | Description |
| --- | --- |
| `v` | variant |
| `va` | variant annotations |
| `aIndex` | allele index |

The following table describes the variables in the scope of `ANNO_EXPR`

| Name | Description |
| --- | --- |
| `v` | the _new_ variant |
| `va` | the _old_ variant annotations |
| `aIndices` | an array of the old allele indices (such that `aIndices[newIndex] = oldIndex` and `aIndices[0] = 0`) |

### Example

The following command filters a list of samples, recompute ACs for each allele, and then removes alternate alleles whose allele count is zero and updates the alternate allele count annotation with the new indices.

```
... \
  filtersamples list --remove -i samples_to_exclude.txt \
  annotatevariants expr -c 'va.AC = gs.map(g => g.oneHotAlleles(v)).sum().map(x => x.toInt), va.info.AN = 2*gs.filter(g => g.isCalled).count().toInt' \
  filteralleles --subset --remove -c 'aIndex > 0 && va.AC[aIndex] == 0' -a 'va.info.AC = aIndices[1:].map(i => va.AC[i])' \
  annotatevariants expr -c 'va.info.AF = va.info.AC / va.info.AN' \
```
Note that here, we must first compute the updated ACs after removing the individuals since aggregators cannot be used within the `-a` expression. When computing the updated ACs here, the AC for the reference allele is also computed. Because the AC values computed include the reference, `aIndices` correspond to the correct alleles.

</div>
