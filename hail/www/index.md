# Hail

Hail is an open-source, general-purpose, Python-based data analysis tool with additional data types and methods for working with genomic data.

Hail is built to scale and has first-class support for multi-dimensional structured data, like the genomic data in a genome-wide association study (GWAS).

Hail is exposed as a Python library, using primitives for distributed queries and linear algebra implemented in Scala, [Spark](https://spark.apache.org/docs/latest/index.html), and increasingly C++.

See the [documentation](https://hail.is/docs/0.2/) for more info on using
Hail. Post to the [Discussion Forum](http://discuss.hail.is) for user support
and feature requests. Chat with the Hail team and user community in Hail's
[Zulip chatroom](https://hail.zulipchat.com).

Hail is actively developed with new features and performance improvements integrated weekly. See the [changelog](https://hail.is/docs/0.2/change_log.html) for more information.

### Maintainer

Hail is maintained by a team in the [Neale lab](https://nealelab.squarespace.com/) at the [Stanley Center for Psychiatric Research](http://www.broadinstitute.org/scientific-community/science/programs/psychiatric-disease/stanley-center-psychiatric-research/stanle) of the [Broad Institute of MIT and Harvard](http://www.broadinstitute.org) and the [Analytic and Translational Genetics Unit](https://www.atgu.mgh.harvard.edu/) of [Massachusetts General Hospital](http://www.massgeneral.org/).

Contact the Hail team at <a href="mailto:hail@broadinstitute.org"><code>hail@broadinstitute.org</code></a>.

### Citing Hail

If you use Hail for published work, please cite the software. You can get a
citation for the version of Hail you installed by executing:

```python
import hail as hl

print(hl.cite_hail())
```

Which will look like:

```
Hail Team. Hail 0.2.13-81ab564db2b4. https://github.com/hail-is/hail/releases/tag/0.2.13.
```

Or if you need a bibtex entry:

```python
import hail as hl

print(hl.cite_hail_bibtex())
```

Which will look like:

```
@misc{Hail,
  author = {Hail Team},
  title = {Hail},
  howpublished = {\url{https://github.com/hail-is/hail/releases/tag/0.2.13}}
}
```

If you simply cannot stomach the idea of citing a GitHub repository (even though
more than 2,800 people have [cited the Keras GitHub
repository](https://scholar.google.com/scholar?cluster=17868569268188187229&hl=en&as_sdt=40000005&sciodt=0,22)),
then please cite this DOI which always points to the latest published version of
Hail:

```
Hail Team. Hail. http://doi.org/10.5281/zenodo.2646680
```

##### Acknowledgements

The Hail team has several sources of funding at the Broad Institute:

 - The Stanley Center for Psychiatric Research, which together with Neale Lab has provided an incredibly supportive and stimulating home.
 - Principal Investigators Benjamin Neale and Daniel MacArthur, whose scientific leadership has been essential for solving the right problems.
 - Jeremy Wertheimer, whose strategic advice and generous philanthropy have been essential for growing the impact of Hail.

We are grateful for generous support from:

 - The National Institute of Diabetes and Digestive and Kidney Diseases
 - The National Institute of Mental Health
 - The National Human Genome Research Institute
 - The Chan Zuckerburg Initiative

We would like to thank <a href="https://zulipchat.com/">Zulip</a> for supporting
open-source by providing free hosting, and YourKit, LLC for generously providing
free licenses for <a href="https://www.yourkit.com/java/profiler/">YourKit Java
Profiler</a> for open-source development.

<img src="https://www.yourkit.com/images/yklogo.png" align="right" />
