from __future__ import print_function  # Python 2 and 3 print compatibility

from hail.java import *
from hail.keytable import KeyTable
from hail.type import Type
from hail.representation import Interval, IntervalTree
from hail.utils import TextTableConfig
from py4j.protocol import Py4JJavaError

import warnings

warnings.filterwarnings(module=__name__, action='once')


class VariantDataset(object):
    """Hail's primary representation of genomic data, a matrix keyed by sample and variant.

    Variant datasets may be generated from other formats using the :py:class:`.HailContext` import methods,
    constructed from a variant-keyed :py:class:`KeyTable` using :py:meth:`.VariantDataset.from_keytable`,
    and simulated using :py:meth:`~hail.HailContext.balding_nichols_model`.

    Once a variant dataset has been written to disk with :py:meth:`~hail.VariantDataset.write`,
     use :py:meth:`~hail.HailContext.read` to load the variant dataset into the environment.

    >>> vds = hc.read("data/example.vds")

    :ivar hc: Hail Context
    :vartype hc: :class:`.HailContext`
    """

    def __init__(self, hc, jvds):
        self.hc = hc
        self._jvds = jvds

        self._globals = None
        self._sample_annotations = None
        self._sa_schema = None
        self._va_schema = None
        self._global_schema = None
        self._genotype_schema = None
        self._sample_ids = None
        self._num_samples = None
        self._jvdf_cache = None

    @staticmethod
    @handle_py4j
    def from_keytable(key_table):
        """Construct a sites-only variant dataset from a key table.

        The key table must be keyed by one column of type :py:class:`.TVariant`.

        All columns in the key table become variant annotations in the result.
        For example, a key table with key column ``v`` (*Variant*) and column
        ``gene`` (*String*) will produce a sites-only variant dataset with a
        ``va.gene`` variant annotation.

        :param key_table: variant-keyed key table
        :type key_table: :py:class:`.KeyTable`

        :return: Sites-only variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """
        if not isinstance(key_table, KeyTable):
            raise TypeError("parameter `key_table' must be a KeyTable, but found %s" % type(key_table))
        jvds = scala_object(Env.hail().variant, 'VariantDataset').fromKeyTable(key_table._jkt)
        return VariantDataset(key_table.hc, jvds)

    @property
    def _jvdf(self):
        if self._jvdf_cache is None:
            self._jvdf_cache = Env.hail().variant.VariantDatasetFunctions(self._jvds)
        return self._jvdf_cache

    @property
    def sample_ids(self):
        """Return sampleIDs.

        :return: List of sample IDs.
        :rtype: list of str
        """

        if self._sample_ids is None:
            self._sample_ids = jiterable_to_list(self._jvds.sampleIds())
        return self._sample_ids

    @property
    def sample_annotations(self):
        """Return a dict of sample annotations.

        The keys of this dictionary are the sample IDs (strings).
        The values are sample annotations.

        :return: dict
        """

        if self._sample_annotations is None:
            zipped_annotations = Env.jutils().iterableToArrayList(
                self._jvds.sampleIdsAndAnnotations()
            )
            r = {}
            for element in zipped_annotations:
                r[element._1()] = self.sample_schema._convert_to_py(element._2())
            self._sample_annotations = r
        return self._sample_annotations

    @handle_py4j
    def num_partitions(self):
        """Number of partitions.

        **Notes**

        The data in a variant dataset is divided into chunks called partitions, which may be stored together or across a network, so that each partition may be read and processed in parallel by available cores. Partitions are a core concept of distributed computation in Spark, see `here <http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds>`_ for details.

        :rtype: int
        """

        return self._jvds.nPartitions()

    @property
    def num_samples(self):
        """Number of samples.

        :rtype: int
        """

        if self._num_samples is None:
            self._num_samples = self._jvds.nSamples()
        return self._num_samples

    @handle_py4j
    def count_variants(self):
        """Count number of variants in variant dataset.

        :rtype: long
        """

        return self._jvds.countVariants()

    @handle_py4j
    def was_split(self):
        """True if multiallelic variants have been split into multiple biallelic variants.

        Result is True if :py:meth:`~hail.VariantDataset.split_multi` or :py:meth:`~hail.VariantDataset.filter_multi` has been called on this variant dataset,
        or if the variant dataset was imported with :py:meth:`~hail.HailContext.import_plink`, :py:meth:`~hail.HailContext.import_gen`,
        or :py:meth:`~hail.HailContext.import_bgen`, or if the variant dataset was simulated with :py:meth:`~hail.HailContext.balding_nichols_model`.

        :rtype: bool
        """

        return self._jvds.wasSplit()

    @handle_py4j
    def is_dosage(self):
        """True if genotype probabilities are dosages.

        The result of ``is_dosage()`` will be True if the variant dataset was imported with :py:meth:`~hail.HailContext.import_gen` or
        :py:meth:`~hail.HailContext.import_bgen`.

        :rtype: bool
        """

        return self._jvds.isDosage()

    @handle_py4j
    def file_version(self):
        """File version of variant dataset.

        :rtype: int
        """

        return self._jvds.fileVersion()

    @handle_py4j
    def aggregate_by_key(self, key_code, agg_code):
        """Aggregate by user-defined key and aggregation expressions to produce a KeyTable.
        Equivalent to a group-by operation in SQL.

        **Examples**

        Compute the number of LOF heterozygote calls per gene per sample:

        >>> kt_result = (vds
        ...     .aggregate_by_key(['Sample = s', 'Gene = va.gene'],
        ...                        'nHet = g.filter(g => g.isHet() && va.consequence == "LOF").count()')
        ...     .export("test.tsv"))

        This will produce a :class:`KeyTable` with 3 columns (`Sample`, `Gene`, `nHet`).

        :param key_code: Named expression(s) for which fields are keys.
        :type key_code: str or list of str

        :param agg_code: Named aggregation expression(s).
        :type agg_code: str or list of str

        :rtype: :class:`.KeyTable`
        """

        if isinstance(key_code, list):
            key_code = ",".join(key_code)
        if isinstance(agg_code, list):
            agg_code = ",".join(agg_code)

        return KeyTable(self.hc, self._jvds.aggregateByKey(key_code, agg_code))

    @handle_py4j
    def aggregate_intervals(self, input, expr, output):
        '''Aggregate over intervals and export.

        **Examples**

        Calculate the total number of SNPs, indels, and variants contained in
        the intervals specified by *data/capture_intervals.txt*:

        >>> vds_result = vds.aggregate_intervals('data/capture_intervals.txt',
        ...   'n_SNP = variants.filter(v => v.altAllele().isSNP()).count(), ' +
        ...   'n_indel = variants.filter(v => v.altAllele().isIndel()).count(), ' +
        ...   'n_total = variants.count()',
        ...   'output/out.txt')

        If *data/capture_intervals.txt* contains:

        .. code-block:: text

            4:1500-123123
            5:1-1000000
            16:29500000-30200000

        then the previous expression writes something like the following to
        *out.txt*:

        .. code-block:: text

            Contig    Start       End         n_SNP   n_indel     n_total
            4         1500        123123      502     51          553
            5         1           1000000     25024   4500        29524
            16        29500000    30200000    17222   2021        19043

        The parameter ``expr`` defines the names of the column headers (in
        the previous case: ``n_SNP``, ``n_indel``, ``n_total``) and how to
        calculate the value of that column for each interval.

        Count the number of LOF, missense, and synonymous non-reference calls
        per interval:

        >>> vds_result = (vds.annotate_variants_expr('va.n_calls = gs.filter(g => g.isCalledNonRef()).count()')
        ...     .aggregate_intervals('data/intervals.txt',
        ...            'LOF_CALLS = variants.filter(v => va.consequence == "LOF").map(v => va.n_calls).sum(), ' +
        ...            'MISSENSE_CALLS = variants.filter(v => va.consequence == "missense").map(v => va.n_calls).sum(), ' +
        ...            'SYN_CALLS = variants.filter(v => va.consequence == "synonymous").map(v => va.n_calls).sum()',
        ...            'output/out.txt'))

        If *data/intervals.txt* contains:

        .. code-block:: text

            4:1500-123123
            5:1-1000000
            16:29500000-30200000

        then the previous expression writes something like the following to
        *out.txt*:

        .. code-block:: text

            Contig    Start       End         LOF_CALLS   MISSENSE_CALLS   SYN_CALLS
            4         1500        123123      42          122              553
            5         1           1000000     3           12               66
            16        29500000    30200000    17          22               202

        **Notes**

        Intervals are **left inclusive, right exclusive**.  This means that
        [chr1:1, chr1:3) contains chr1:1 and chr1:2.

        **Designating output with an expression**

        An export expression designates a list of computations to perform, and
        what these columns are named.  These expressions should take the form
        ``COL_NAME_1 = <expression>, COL_NAME_2 = <expression>, ...``.

        ``expr`` has the following symbols in scope:

        - ``interval`` (*Interval*): genomic interval
        - ``global``: global annotations
        - ``variants`` (*Aggregable[Variant]*): aggregable of :py:class:`~hail.representation.Variant` s Aggregator namespace below.

        The ``variants`` aggregator has the following namespace:

        - ``v`` (*Variant*): :ref:`variant`
        - ``va``: variant annotations
        - ``global``: Global annotations

        :param str input: Input interval list file.

        :param str expr: Export expression.

        :param str output: Output file.
        '''

        self._jvds.aggregateIntervals(input, expr, output)

    @handle_py4j
    def annotate_alleles_expr(self, expr, propagate_gq=False):
        """Annotate alleles with expression.

        **Examples**

        To create a variant annotation ``va.nNonRefSamples: Array[Int]`` where the ith entry of
        the array is the number of samples carrying the ith alternate allele:

        >>> vds_result = vds.annotate_alleles_expr('va.nNonRefSamples = gs.filter(g => g.isCalledNonRef()).count()')

        **Notes**

        This command is similar to :py:meth:`.annotate_variants_expr`. :py:meth:`.annotate_alleles_expr` dynamically splits multi-allelic sites,
        evaluates each expression on each split allele separately, and for each expression annotates with an array with one element per alternate allele. In the splitting, genotypes are downcoded and each alternate allele is represented
        using its minimal representation (see :py:meth:`split_multi` for more details).


        :param expr: Annotation expression.
        :type expr: str or list of str
        :param bool propagate_gq: Propagate GQ instead of computing from (split) PL.

        :return: Annotated variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """
        if isinstance(expr, list):
            expr = ",".join(expr)

        jvds = self._jvdf.annotateAllelesExpr(expr, propagate_gq)
        return VariantDataset(self.hc, jvds)


    @handle_py4j
    def annotate_alleles_vds(self, vds, code, match_star = True):
        if isinstance(code, list):
            expr = ",".join(code)
        jvds = self._jvdf.annotateAllelesVDS(vds._jvds, code, match_star)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_global_expr(self, expr):
        """Annotate global with expression.

        **Example**

        Annotate global with an array of populations:

        >>> vds = vds.annotate_global_expr('global.pops = ["FIN", "AFR", "EAS", "NFE"]')

        Create, then overwrite, then drop a global annotation:

        >>> vds = vds.annotate_global_expr('global.pops = ["FIN", "AFR", "EAS"]')
        >>> vds = vds.annotate_global_expr('global.pops = ["FIN", "AFR", "EAS", "NFE"]')
        >>> vds = vds.annotate_global_expr('global.pops = drop(global, pops)')

        The expression namespace contains only one variable:

        - ``global``: global annotations

        :param expr: Annotation expression
        :type expr: str or list of str

        :return: Annotated variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        if isinstance(expr, list):
            expr = ','.join(expr)

        jvds = self._jvds.annotateGlobalExpr(expr)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_global_py(self, path, annotation, annotation_type):
        """Annotate global from Python objects.

        **Example**

        >>> vds_result = vds.annotate_global_py('global.populations',
        ...                                     ['EAS', 'AFR', 'EUR', 'SAS', 'AMR'],
        ...                                     TArray(TString()))

        **Notes**

        This method registers new global annotations in a VDS. These annotations
        can then be accessed through expressions in downstream operations. The
        Hail data type must be provided and must match the given ``annotation``
        parameter.

        :param str path: annotation path starting in 'global'

        :param annotation: annotation to add to global

        :param annotation_type: Hail type of annotation
        :type annotation_type: :py:class:`.Type`

        :return: Annotated variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        annotation_type._typecheck(annotation)

        annotated = self._jvds.annotateGlobal(annotation_type._convert_to_j(annotation), annotation_type._jtype, path)
        assert annotated.globalSignature().typeCheck(annotated.globalAnnotation()), 'error in java type checking'
        return VariantDataset(self.hc, annotated)

    @handle_py4j
    def annotate_global_list(self, input, root, as_set=False):
        """Load text file into global annotations as Array[String] or
        Set[String].

        **Examples**

        Add a list of genes in a file to global annotations:

        >>> vds_result = vds.annotate_global_list('data/genes.txt', 'global.genes')

        For the gene list

        .. code-block: text

            $ cat data/genes.txt
            SCN2A
            SONIC-HEDGEHOG
            PRNP

        this adds ``global.genes: Array[String]`` with value ``["SCN2A", "SONIC-HEDGEHOG", "PRNP"]``.

        To filter to those variants in genes listed in *genes.txt* given a variant annotation ``va.gene: String``, annotate as type ``Set[String]`` instead:

        >>> vds_result = (vds.annotate_global_list('data/genes.txt', 'global.genes', as_set=True)
        ...     .filter_variants_expr('global.genes.contains(va.gene)'))

        :param str input: Input text file.

        :param str root: Global annotation path to store text file.

        :param bool as_set: If True, load text file as Set[String],
            otherwise, load as Array[String].

        :return: Annotated variant dataset with new global annotation given by the list.
        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvds.annotateGlobalList(input, root, as_set)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_global_table(self, input, root, config=TextTableConfig()):
        """Load delimited text file (text table) into global annotations as
        Array[Struct].

        **Examples**

        Load a file as a global annotation.  Consider the file *data/genes_pli_exac.txt* with contents:

        .. code-block:: text

          GENE    PLI     EXAC_LOF_COUNT
          Gene1   0.12312 2
          ...

        >>> vds_result = vds.annotate_global_table('data/genes_pli_exac.txt', 'global.genes',
        ...                           config=TextTableConfig(types='PLI: Double, EXAC_LOF_COUNT: Int'))

        creates a new global annotation ``global.genes`` with type:

        .. code-block:: text

          global.genes: Array[Struct {
              GENE: String,
              PLI: Double,
              EXAC_LOF_COUNT: Int
          }]

        where each line is stored as an element of the array.

        **Notes**

        :param str input: Input text file.

        :param str root: Global annotation path to store text table.

        :param config: Configuration options for importing text files
        :type config: :class:`.TextTableConfig`

        :return: Annotated variant dataset.
        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvds.annotateGlobalTable(input, root, config._to_java())
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_samples_expr(self, expr):
        """Annotate samples with expression.

        **Examples**

        Compute per-sample GQ statistics for hets:

        >>> vds_result = (vds.annotate_samples_expr('sa.gqHetStats = gs.filter(g => g.isHet()).map(g => g.gq).stats()')
        ...     .export_samples('output/samples.txt', 'sample = s, het_gq_mean = sa.gqHetStats.mean'))

        Compute the list of genes with a singleton LOF per sample:

        >>> vds_result = (vds.annotate_variants_table('data/consequence.tsv', 'Variant', code='va.consequence = table.Consequence', config=TextTableConfig(impute=True))
        ...     .annotate_variants_expr('va.isSingleton = gs.map(g => g.nNonRefAlleles()).sum() == 1')
        ...     .annotate_samples_expr('sa.LOF_genes = gs.filter(g => va.isSingleton && g.isHet() && va.consequence == "LOF").map(g => va.gene).collect()'))

        To create an annotation for only a subset of samples based on an existing annotation:

        >>> vds_result = vds.annotate_samples_expr('sa.newpheno = if (sa.pheno.cohortName == "cohort1") sa.pheno.bloodPressure else NA: Double')

        .. note::

            For optimal performance, be sure to explicitly give the alternative (``NA``) the same type as the consequent (``sa.pheno.bloodPressure``).

        **Notes**

        ``expr`` is in sample context so the following symbols are in scope:

        - ``s`` (*Sample*): :ref:`sample`
        - ``sa``: sample annotations
        - ``global``: global annotations
        - ``gs`` (*Aggregable[Genotype]*): aggregable of :ref:`genotype` for sample ``s``

        :param expr: Annotation expression.
        :type expr: str or list of str

        :return: Annotated variant dataset.
        :rtype: :class:`.VariantDataset`
        """

        if isinstance(expr, list):
            expr = ','.join(expr)

        jvds = self._jvds.annotateSamplesExpr(expr)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_samples_fam(self, input, quantpheno=False, delimiter='\\\\s+', root='sa.fam', missing='NA'):
        """Import PLINK .fam file into sample annotations.

        **Examples**

        Import case-control phenotype data from a tab-separated `PLINK .fam
        <https://www.cog-genomics.org/plink2/formats#fam>`_ file into sample
        annotations:

        >>> vds_result = vds.annotate_samples_fam("data/myStudy.fam")

        In Hail, unlike PLINK, the user must *explicitly* distinguish between
        case-control and quantitative phenotypes. Importing a quantitative
        phenotype without ``quantpheno=True`` will return an error
        (unless all values happen to be ``0``, ``1``, ``2``, and ``-9``):

        >>> vds_result = vds.annotate_samples_fam("data/myStudy.fam", quantpheno=True)

        **Annotations**

        The annotation names, types, and missing values are shown below,
        assuming the default root ``sa.fam``.

        - **sa.fam.famID** (*String*) -- Family ID (missing = "0")
        - **s** (*String*) -- Sample ID
        - **sa.fam.patID** (*String*) -- Paternal ID (missing = "0")
        - **sa.fam.matID** (*String*) -- Maternal ID (missing = "0")
        - **sa.fam.isFemale** (*Boolean*) -- Sex (missing = "NA", "-9", "0")
        - **sa.fam.isCase** (*Boolean*) -- Case-control phenotype (missing = "0", "-9", non-numeric or the ``missing`` argument, if given.
        - **sa.fam.qPheno** (*Double*) -- Quantitative phenotype (missing = "NA" or the ``missing`` argument, if given.

        :param str input: Path to .fam file.

        :param str root: Sample annotation path to store .fam file.

        :param bool quantpheno: If True, .fam phenotype is interpreted as quantitative.

        :param str delimiter: .fam file field delimiter regex.

        :param str missing: The string used to denote missing values.
            For case-control, 0, -9, and non-numeric are also treated
            as missing.

        :return: Annotated variant dataset with sample annotations from fam file.
        :rtype: :class:`.VariantDataset`
        """

        ffc = Env.hail().io.plink.FamFileConfig(quantpheno, delimiter, missing)
        jvds = self._jvds.annotateSamplesFam(input, root, ffc)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_samples_list(self, input, root):
        """Annotate samples with a Boolean indicating presence in a list of samples in a text file.

        **Examples**

        Add the sample annotation ``sa.inBatch1: Boolean`` with value true if the sample is in *batch1.txt*:

        >>> vds_result = vds.annotate_samples_list('data/batch1.txt','sa.inBatch1')

        The file must have no header and one sample per line

        .. code-block: text

            $ cat data/batch1.txt
            SampleA
            SampleB
            ...

        :param str input: Sample list file.

        :param str root: Sample annotation path to store Boolean.

        :return: Annotated variant dataset with a new boolean sample annotation
        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvds.annotateSamplesList(input, root)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_samples_table(self, input, sample_expr, root=None, code=None, config=TextTableConfig()):
        """Annotate samples with delimited text file (text table).

        **Examples**

        To annotates samples using `samples1.tsv` with type imputation::

        >>> conf = TextTableConfig(impute=True)
        >>> vds_result = vds.annotate_samples_table('data/samples1.tsv', 'Sample', root='sa.pheno', config=conf)

        Given this file

        .. code-block: text

            $ cat data/samples1.tsv
            Sample	Height	Status  Age
            PT-1234	154.1	ADHD	24
            PT-1236	160.9	Control	19
            PT-1238	NA	ADHD	89
            PT-1239	170.3	Control	55

        the three new sample annotations are ``sa.pheno.Height: Double``, ``sa.pheno.Status: String``, and ``sa.pheno.Age: Int``.

        To annotate without type imputation, resulting in all String types:

        >>> vds_result = vds.annotate_samples_table('data/samples1.tsv', 'Sample', root='sa.phenotypes')

        **Detailed examples**

        Let's import annotations from a CSV file with missing data and special characters

        .. code-block: text

            $ cat data/samples2.tsv
            Batch,PT-ID
            1kg,PT-0001
            1kg,PT-0002
            study1,PT-0003
            study3,PT-0003
            .,PT-0004
            1kg,PT-0005
            .,PT-0006
            1kg,PT-0007

        In this case, we should:

        - Escape the ``PT-ID`` column with backticks in the ``sample_expr`` argument because it contains a dash

        - Pass the non-default delimiter ``,``

        - Pass the non-default missing value ``.``

        - Add the only useful column using ``code`` rather than the ``root`` parameter.

        >>> conf = TextTableConfig(delimiter=',', missing='.')
        >>> vds_result = vds.annotate_samples_table('data/samples2.tsv', '`PT-ID`', code='sa.batch = table.Batch', config=conf)

        Let's import annotations from a file with no header and sample IDs that need to be transformed. Suppose the vds sample IDs are of the form ``NA#####``. This file has no header line, and the sample ID is hidden in a field with other information

        .. code-block: text

            $ cat data/samples3.tsv
            1kg_NA12345   female
            1kg_NA12346   male
            1kg_NA12348   female
            pgc_NA23415   male
            pgc_NA23418   male

        To import it:

        >>> conf = TextTableConfig(noheader=True)
        >>> vds_result = vds.annotate_samples_table('data/samples3.tsv',
        ...                             '_0.split("_")[1]',
        ...                             code='sa.sex = table._1, sa.batch = table._0.split("_")[0]',
        ...                             config=conf)

        **Using the** ``sample_expr`` **argument**

        This argument tells Hail how to get a sample ID out of your table. Each column in the table is exposed to the Hail expr language. Possibilities include ``Sample`` (if your sample id is in a column called 'Sample'), ``_2`` (if your sample ID is the 3rd column of a table with no header), or something more complicated like ``'if ("PGC" ~ ID1) ID1 else ID2'``.  All that matters is that this expr results in a string.  If the expr evaluates to missing, it will not be mapped to any VDS samples.

        **Using the** ``root`` **and** ``code`` **arguments**

        This module requires exactly one of these two arguments to tell Hail how to insert the table into the sample annotation schema.

        The ``root`` argument is the simpler of these two, and simply packages up all table annotations as a ``Struct`` and drops it at the given ``root`` location.  If your table has columns ``Sample``, ``Sex``, and ``Batch``, then ``root='sa.metadata'`` creates the struct ``{Sample, Sex, Batch}`` at ``sa.metadata``, which gives you access to the paths ``sa.metadata.Sample``, ``sa.metadata.Sex``, and ``sa.metadata.Batch``.

        The ``code`` argument expects an annotation expression and has access to ``sa`` (the sample annotations in the VDS) and ``table`` (a struct with all the columns in the table).  ``root='sa.anno'`` is equivalent to ``code='sa.anno = table'``.

        **Common uses for the** ``code`` **argument**

        Don't generate a full struct in a table with only one annotation column

        .. code-block: text

            code='sa.annot = table._1'

        Put annotations on the top level under `sa`

        .. code-block: text

            code='sa = merge(sa, table)'

        Load only specific annotations from the table

        .. code-block: text

            code='sa.annotations = select(table, toKeep1, toKeep2, toKeep3)'

        The above is equivalent to

        .. code-block: text

            code='sa.annotations.toKeep1 = table.toKeep1,
                sa.annotations.toKeep2 = table.toKeep2,
                sa.annotations.toKeep3 = table.toKeep3'


        :param str input: Path to delimited text file.

        :param str sample_expr: Expression for sample id (key).

        :param str root: Sample annotation path to store text table.

        :param str code: Annotation expression.

        :param config: Configuration options for importing text files
        :type config: :class:`.TextTableConfig`

        :return: Annotated variant dataset with new samples annotations imported from a text file
        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvds.annotateSamplesTable(input, sample_expr, joption(root), joption(code), config._to_java())
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_samples_vds(self, right, root=None, code=None):
        """Annotate samples with sample annotations from .vds file.

        :param right: Dataset to annotate with.
        :type right: :py:class:`.VariantDataset`

        :param str root: Sample annotation path to add sample annotations.

        :param str code: Annotation expression.

        :return: Annotated variant dataset.
        :rtype: :class:`.VariantDataset`

        """

        jvds = self._jvds.annotateSamplesVDS(right._jvds, joption(root), joption(code))
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_samples_keytable(self, keytable, expr, vds_key=None):
        """Annotate samples with a :py:class:`.KeyTable`.

        If `vds_key` is None, the key table must have exactly one key of
        type *String*.

        If `vds_key` is not None, it must be a list of Hail expressions whose types
        match, in order, the `keytable`'s key types.

        **Examples**

        Add annotations from a sample-keyed TSV:

        >>> kt = hc.import_keytable('data/samples2.tsv',
        ...                         config=TextTableConfig(impute=True, delimiter=",")).key_by('PT-ID')
        ... annotate_vds = vds.annotate_samples_keytable(kt, expr='sa.batch = table.Batch')

        **Notes**

        ``expr`` has the following symbols in scope:

          - ``sa``: sample annotations
          - ``table``: :py:class:`.KeyTable` value

        each expression in the list ``vds_key`` has the following symbols in
        scope:

          - ``s`` (*Sample*): :ref:`sample`
          - ``sa``: sample annotations

        :param keytable: Key table with which to annotate samples.
        :type keytable: :class:`.KeyTable`

        :param str expr: Annotation expression.

        :param vds_key: Join key(s) in the dataset. Sample ID is used if this parameter is None.
        :type vds_key: list of str, str, or None

        :rtype: :class:`VariantDataset`
        """

        if isinstance(expr, list):
            expr = ','.join(expr)

        if vds_key is None:
            jvds = self._jvds.annotateSamplesKeyTable(keytable._jkt, expr)
        else:
            if not isinstance(vds_key, list):
                vds_key = [vds_key]
            jvds = self._jvds.annotateSamplesKeyTable(keytable._jkt, vds_key, expr)

        return VariantDataset(self.hc, jvds)



    @handle_py4j
    def annotate_variants_bed(self, input, root, all=False):
        """Annotate variants based on the intervals in a .bed file.

        **Examples**

        Add the variant annotation ``va.cnvRegion: Boolean`` indicating inclusion in at least one interval of the three-column BED file `file1.bed`:

        >>> vds_result = vds.annotate_variants_bed('data/file1.bed', 'va.cnvRegion')

        Add a variant annotation ``va.cnvRegion: String`` with value given by the fourth column of `file2.bed`:

        >>> vds_result = vds.annotate_variants_bed('data/file2.bed', 'va.cnvRegion')

        The file formats are

        .. code-block: text

            $ cat data/file1.bed
            track name="BedTest"
            20    1          14000000
            20    17000000   18000000
            ...

            $ cat file2.bed
            track name="BedTest"
            20    1          14000000  cnv1
            20    17000000   18000000  cnv2
            ...


        **Notes**

        `UCSC bed files <https://genome.ucsc.edu/FAQ/FAQformat.html#format1>`_ can have up to 12 fields, but Hail will only ever look at the first four.  The first three fields are required (``chrom``, ``chromStart``, and ``chromEnd``).  If a fourth column is found, Hail will parse this field as a string and load it into the specified annotation path.  If the bed file has only three columns, Hail will assign each variant a Boolean annotation, true if and only if the variant lies in the union of the intervals. Hail ignores header lines in BED files.

        If the ``all`` parameter is set to ``True`` and a fourth column is present, the annotation will be the set (possibly empty) of fourth column strings as a ``Set[String]`` for all intervals that overlap the given variant.

        .. caution:: UCSC BED files are end-exclusive but 0-indexed, so the line "5  100  105" is interpreted in Hail as loci `5:101, 5:102, 5:103, 5:104. 5:105`. Details `here <http://genome.ucsc.edu/blog/the-ucsc-genome-browser-coordinate-counting-systems/>`_.

        :param str input: Path to .bed file.

        :param str root: Variant annotation path to store annotation.

        :param bool all: Store values from all overlapping intervals as a set.

        :return: Annotated variant dataset with new variant annotations imported from a .bed file.
        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvds.annotateVariantsBED(input, root, all)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_variants_expr(self, expr):
        """Annotate variants with expression.

        **Examples**

        Compute GQ statistics about heterozygotes per variant:

        >>> vds_result = vds.annotate_variants_expr('va.gqHetStats = gs.filter(g => g.isHet()).map(g => g.gq).stats()')

        Collect a list of sample IDs with non-ref calls in LOF variants:

        >>> vds_result = vds.annotate_variants_expr('va.nonRefSamples = gs.filter(g => g.isCalledNonRef()).map(g => s.id).collect()')

        Substitute a custom string for the rsID field:

        >>> vds_result = vds.annotate_variants_expr('va.rsid = str(v)')

        **Notes**

        ``expr`` is in variant context so the following symbols are in scope:

          - ``v`` (*Variant*): :ref:`variant`
          - ``va``: variant annotations
          - ``global``: global annotations
          - ``gs`` (*Aggregable[Genotype]*): aggregable of :ref:`genotype` for variant ``v``

        For more information, see the documentation on writing `expressions <overview.html#expressions>`_
        and using the `Hail Expression Language <exprlang.html>`_.

        :param expr: Annotation expression or list of annotation expressions.
        :type expr: str or list of str

        :return: Annotated variant dataset.
        :rtype: :class:`.VariantDataset`
        """

        if isinstance(expr, list):
            expr = ','.join(expr)

        jvds = self._jvds.annotateVariantsExpr(expr)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_variants_keytable(self, keytable, expr, vds_key=None):
        """Annotate variants with an expression that may depend on a :py:class:`.KeyTable`.

        If `vds_key` is None, the keytable's key must be exactly one column and
        that column must have type *Variant*.

        If `vds_key` is not None, it must be a list of Hail expressions whose types
        match, in order, the `keytable`'s key type.

        **Examples**

        Add annotations from a variant-keyed TSV:

        >>> kt = hc.import_keytable('data/variant-lof.tsv', config=TextTableConfig(impute=True)).key_by('v')
        >>> vds_result = vds.annotate_variants_keytable(kt, 'va.lof = table.lof')

        Add annotations from a gene-and-type-keyed TSV:

        >>> kt = hc.import_keytable('data/locus-metadata.tsv',
        ...                         config=TextTableConfig(impute=True)).key_by(['gene', 'type'])
        >>>
        >>> vds_result = (vds.annotate_variants_keytable(kt,
        ...       'va.foo = table.foo',
        ...       ['va.gene', 'if (va.score > 10) "Type1" else "Type2"']))

        **Notes**

        ``expr`` has the following symbols in scope:

          - ``va``: variant annotations
          - ``table``: :py:class:`.KeyTable` value

        each expression in the list ``vds_key`` has the following symbols in
        scope:

          - ``v`` (*Variant*): :ref:`variant`
          - ``va``: variant annotations

        :param expr: Annotation expression or list of annotation expressions
        :type expr: str or list of str

        :param vds_key: A list of annotation expressions to be used as the VDS's join key
        :type vds_key: None or list of str

        :return: A :py:class:`.VariantDataset` with new variant annotations specified by ``expr``

        :rtype: :py:class:`.VariantDataset`
        """

        if isinstance(expr, list):
            expr = ','.join(expr)

        if vds_key is None:
            jvds = self._jvds.annotateVariantsKeyTable(keytable._jkt, expr)
        else:
            jvds = self._jvds.annotateVariantsKeyTable(keytable._jkt, vds_key, expr)

        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_variants_intervals(self, input, root, all=False):
        """Annotate variants from an interval list file.

        **Examples**

        Consider the file, *data/exons.interval_list*, in
        ``chromosome:start-end`` format:

        .. code-block:: text

            $ cat data/exons.interval_list
            1:5122980-5123054
            1:5531412-5531715
            1:5600022-5601025
            1:5610246-5610349

        The following invocation produces a vds with a new variant annotation,
        ``va.inExon``. The annotation ``va.inExon`` is ``true`` for every
        variant included by ``exons.interval_list`` and false otherwise.

        >>> vds_result = vds.annotate_variants_intervals('data/exons.interval_list', 'va.inExon')

        Consider the tab-separated, five-column file *data/exons2.interval_list*:

        .. code-block:: text

            $ cat data/exons2.interval_list
            1   5122980 5123054 + gene1
            1   5531412 5531715 + gene1
            1   5600022 5601025 - gene2
            1   5610246 5610349 - gene2

        This file maps from variant intervals to gene names. The following
        invocation produces a vds with a new variant annotation ``va.gene``. The
        annotation ``va.gene`` is set to the gene name occurring in the fifth
        column and ``NA`` otherwise.

        >>> vds_result = vds.annotate_variants_intervals('data/exons2.interval_list', 'va.gene')

        **Notes**

        There are two formats for interval list files.  The first appears as
        ``chromosome:start-end`` as in the first example.  This format will
        annotate variants with a *Boolean*, which is ``true`` if that variant is
        found in any interval specified in the file and `false` otherwise.

        The second interval list format is a TSV with fields chromosome, start,
        end, strand, target.  **There should not be a header.** This file will
        annotate variants with the *String* in the fifth column (target). If
        ``all=True``, the annotation will be the, possibly empty,
        ``Set[String]`` of fifth column strings (targets) for all intervals that
        overlap the given variant.

        :param str input: Path to .interval_list.

        :param str root: Variant annotation path to store annotation.

        :param bool all: If true, store values from all overlapping
            intervals as a set.

        :return: Annotated variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvds.annotateVariantsIntervals(input, root, all)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_variants_loci(self, path, locus_expr, root=None, code=None, config=TextTableConfig()):
        """Annotate variants from an delimited text file (text table) indexed
        by loci.

        :param str path: Path to delimited text file.

        :param str locus_expr: Expression for locus (key).

        :param str root: Variant annotation path to store annotation.

        :param str code: Annotation expression.

        :param config: Configuration options for importing text files
        :type config: :class:`.TextTableConfig`

        :return: Annotated variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvds.annotateVariantsLoci(path, locus_expr, joption(root), joption(code), config._to_java())
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_variants_table(self, path, variant_expr, root=None, code=None, config=TextTableConfig()):
        """Annotate variant with delimited text file (text table).

        :param path: Path to delimited text files.
        :type path: str or list of str

        :param str variant_expr: Expression for Variant (key).

        :param str root: Variant annotation path to store text table.

        :param str code: Annotation expression.

        :param config: Configuration options for importing text files
        :type config: :class:`.TextTableConfig`

        :return: Annotated variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvds.annotateVariantsTable(path, variant_expr, joption(root), joption(code), config._to_java())
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def annotate_variants_vds(self, other, code=None, root=None):
        '''Annotate variants with variant annotations from .vds file.

        **Examples**

        Import a second variant dataset with annotations to merge into ``vds``:

        >>> vds1 = vds.annotate_variants_expr('va = drop(va, anno1)')
        >>> vds2 = (hc.read("data/example2.vds")
        ...           .annotate_variants_expr('va = select(va, anno1, toKeep1, toKeep2, toKeep3)'))

        Copy the ``anno1`` annotation from ``other`` to ``va.annot``:

        >>> vds_result = vds1.annotate_variants_vds(vds2, code='va.annot = vds.anno1')

        Merge the variant annotations from the two vds together and places them
        at ``va``:

        >>> vds_result = vds1.annotate_variants_vds(vds2, code='va = merge(va, vds)')

        Select a subset of the annotations from ``other``:

        >>> vds_result = vds1.annotate_variants_vds(vds2, code='va.annotations = select(vds, toKeep1, toKeep2, toKeep3)')

        The previous expression is equivalent to:

        >>> vds_result = vds1.annotate_variants_vds(vds2, code='va.annotations.toKeep1 = vds.toKeep1, ' +
        ...                                       'va.annotations.toKeep2 = vds.toKeep2, ' +
        ...                                       'va.annotations.toKeep3 = vds.toKeep3')

        **Notes**

        Using this method requires one of the two optional arguments: ``code``
        and ``root``. They specify how to insert the annotations from ``other``
        into the this vds's variant annotations.

        The ``root`` argument copies all the variant annotations from ``other``
        to the specified annotation path.

        The ``code`` argument expects an annotation expression whose scope
        includes, ``va``, the variant annotations in the current VDS, and ``vds``,
        the variant annotations in ``other``.

        VDSes with multi-allelic variants may produce surprising results because
        all alternate alleles are considered part of the variant key. For
        example:

        - The variant ``22:140012:A:T,TTT`` will not be annotated by
          ``22:140012:A:T`` or ``22:140012:A:TTT``

        - The variant ``22:140012:A:T`` will not be annotated by
          ``22:140012:A:T,TTT``

        It is possible that an unsplit variant dataset contains no multiallelic
        variants, so ignore any warnings Hail prints if you know that to be the
        case.  Otherwise, run :py:meth:`.split_multi` before
        :py:meth:`.annotate_variants_vds`.

        :param VariantDataset other: Variant dataset to annotate with.

        :param str root: Sample annotation path to add variant annotations.

        :param str code: Annotation expression.

        :return: Annotated variant dataset.
        :rtype: :py:class:`.VariantDataset`
        '''

        jvds = self._jvds.annotateVariantsVDS(other._jvds, joption(root), joption(code))

        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def cache(self):
        """Mark this variant dataset to be cached in memory.

        :py:meth:`~hail.VariantDataset.cache` is the same as :func:`persist("MEMORY_ONLY") <hail.VariantDataset.persist>`.
        """

        self._jvdf.cache()
        return self

    @handle_py4j
    def concordance(self, right):
        """Calculate call concordance with another variant dataset.

        **Example**

        >>> concordance_pair = vds.concordance(hc.read('data/example2.vds'))

        **Notes**

        The `concordance` command computes the genotype call concordance between two bialellic variant datasets. The concordance
        results are stored in a global annotation of type Array[Array[Long]], which is a 5x5 table of counts with the
        following mapping:

        0. No Data (missing variant)
        1. No Call (missing genotype call)
        2. Hom Ref
        3. Heterozygous
        4. Hom Var

        The first index in array is the left dataset, the second is the right. For example, ``concordance[3][2]`` is the count of
        genotypes which were heterozygous on the left and homozygous reference on the right. This command produces two new datasets
        and returns them as a Python tuple. The first dataset contains the concordance statistics per variant. This dataset
        **contains no genotypes** (sites-only). It contains a new variant annotation, ``va.concordance``. This is the concordance
        table for each variant in the outer join of the two datasets -- if the variant is present in only one dataset, all
        of the counts will lie in the axis ``va.concordance[0][:]`` (if it is missing on the left) or ``va.concordance.map(x => x[0])``
        (if it is missing on the right). The variant annotations from the left and right datasets are included as ``va.left``
        and ``va.right`` -- these will be missing on one side if a variant was only present in one dataset. This vds also contains
        the global concordance statistics in ``global.concordance``, as well as the left and right global annotations in ``global.left``
        and ``global.right``. The second dataset contains the concordance statistics per sample. This dataset **contains no variants**
        (samples-only). It contains a new sample annotation, ``sa.concordance``. This is a concordance table whose sum is the total number
        of variants in the outer join of the two datasets. The sum ``sa[0].sum`` is equal to the number of variants in the right dataset
        but not the left, and the sum ``sa.concordance.map(x => x[0]).sum)`` is equal to the number of variants in the left dataset but
        not the right. The sample annotations from the left and right datasets are included as ``sa.left`` and ``sa.right``. This dataset
        also contains the global concordance statistics in ``global.concordance``, as well as the left and right global annotations in
        ``global.left`` and ``global.right``.

        **Notes**

        Performs inner join on variants, outer join on samples.

        :param right: right hand variant dataset for concordance
        :type right: :class:`.VariantDataset`

        :return: The global concordance stats, a variant dataset with sample concordance
            statistics, and a variant dataset with variant concordance statistics.
        :rtype: (list of list of int, :py:class:`.VariantDataset`, :py:class:`.VariantDataset`)
        """

        r = self._jvdf.concordance(right._jvds)
        j_global_concordance = r._1()
        sample_vds = VariantDataset(self.hc, r._2())
        variant_vds = VariantDataset(self.hc, r._3())
        global_concordance = [[j_global_concordance.apply(j).apply(i) for i in xrange(5)] for j in xrange(5)]

        return global_concordance, sample_vds, variant_vds

    @handle_py4j
    def count(self, genotypes=False):
        """Returns number of samples, variants and genotypes in this vds as a dictionary with keys
        ``'nSamples'``, ``'nVariants'``, and ``'nGenotypes'``.

        :param bool genotypes: If True, include number of called
            genotypes and genotype call rate as keys ``'nCalled'`` and ``'callRate'``, respectively.

        :rtype: dict
        """

        return dict(self._jvdf.count(genotypes).toJavaMap())

    @handle_py4j
    def deduplicate(self):
        """Remove duplicate variants.

        :return: Deduplicated variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        return VariantDataset(self.hc, self._jvds.deduplicate())

    @handle_py4j
    def downsample_variants(self, keep):
        """Downsample variants.

        :param int keep: (Expected) number of variants to keep.

        :return: Downsampled variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        return VariantDataset(self.hc, self._jvds.downsampleVariants(keep))

    @handle_py4j
    def export_gen(self, output):
        """Export variant dataset as GEN and SAMPLE file.

        **Examples**

        Import dosage data, filter variants based on INFO score, and export data to a GEN and SAMPLE file:

        >>> vds3 = hc.import_bgen("data/example3.bgen", sample_file="data/example3.sample")

        >>> (vds3.filter_variants_expr("gs.infoScore().score >= 0.9")
        ...      .export_gen("output/infoscore_filtered"))

        **Notes**

        Writes out the internal VDS to a GEN and SAMPLE fileset in the `Oxford spec <http://www.stats.ox.ac.uk/%7Emarchini/software/gwas/file_format.html>`_.

        The first 6 columns of the resulting GEN file are the following:

        - Chromosome (``v.contig``)
        - Variant ID (``va.varid`` if defined, else Chromosome:Position:Ref:Alt)
        - rsID (``va.rsid`` if defined, else ".")
        - position (``v.start``)
        - reference allele (``v.ref``)
        - alternate allele (``v.alt``)

        Probability dosages:

        - 3 probabilities per sample ``(pHomRef, pHet, pHomVar)``.
        - Any filtered genotypes will be output as ``(0.0, 0.0, 0.0)``.
        - If the input data contained Phred-scaled likelihoods, the probabilities in the GEN file will be the normalized genotype probabilities assuming a uniform prior.
        - If the input data did not have genotype probabilities such as data imported using :py:meth:`~hail.HailContext.import_plink`, all genotype probabilities will be ``(0.0, 0.0, 0.0)``.

        The sample file has 3 columns:

        - ID_1 and ID_2 are identical and set to the sample ID (``s.id``).
        - The third column ("missing") is set to 0 for all samples.

        :param str output: Output file base.  Will write GEN and SAMPLE files.
        """

        self._jvdf.exportGen(output)

    @handle_py4j
    def export_genotypes(self, output, expr, types=False, export_ref=False, export_missing=False):
        """Export genotype-level information to delimited text file.

        **Examples**

        Export genotype information with identifiers that form the header:

        >>> vds.export_genotypes('output/genotypes.tsv', 'SAMPLE=s, VARIANT=v, GQ=g.gq, DP=g.dp, ANNO1=va.anno1, ANNO2=va.anno2')

        Export the same information without identifiers, resulting in a file with no header:

        >>> vds.export_genotypes('output/genotypes.tsv', 's, v, s.id, g.dp, va.anno1, va.anno2')

        **Notes**

        :py:meth:`~hail.VariantDataset.export_genotypes` outputs one line per cell (genotype) in the data set, though HomRef and missing genotypes are not output by default. Use the ``export_ref`` and ``export_missing`` parameters to force export of HomRef and missing genotypes, respectively.

        The ``expr`` argument is a comma-separated list of fields or expressions, all of which must be of the form ``IDENTIFIER = <expression>``, or else of the form ``<expression>``.  If some fields have identifiers and some do not, Hail will throw an exception. The accessible namespace includes ``g``, ``s``, ``sa``, ``v``, ``va``, and ``global``.

        :param str output: Output path.

        :param str expr: Export expression for values to export.

        :param bool types: Write types of exported columns to a file at (output + ".types")

        :param bool export_ref: If True, export reference genotypes.

        :param bool export_missing: If True, export missing genotypes.
        """

        self._jvdf.exportGenotypes(output, expr, types, export_ref, export_missing)

    @handle_py4j
    def export_plink(self, output, fam_expr='id = s.id'):
        """Export variant dataset as `PLINK2 <https://www.cog-genomics.org/plink2/formats>`_ BED, BIM and FAM.

        **Examples**

        Import data from a VCF file, split multi-allelic variants, and export to a PLINK binary file:

        >>> vds.split_multi().export_plink('output/plink')

        **Notes**

        ``fam_expr`` can be used to set the fields in the FAM file.
        The following fields can be assigned:

        - ``famID: String``
        - ``id: String``
        - ``matID: String``
        - ``patID: String``
        - ``isFemale: Boolean``
        - ``isCase: Boolean`` or ``qPheno: Double``

        If no assignment is given, the value is missing and the
        missing value is used: ``0`` for IDs and sex and ``-9`` for
        phenotype.  Only one of ``isCase`` or ``qPheno`` can be
        assigned.

        ``fam_expr`` is in sample context only and the following
        symbols are in scope:

        - ``s`` (*Sample*): :ref:`sample`
        - ``sa``: sample annotations
        - ``global``: global annotations

        The BIM file ID field is set to ``CHR:POS:REF:ALT``.

        This code:

        >>> vds.split_multi().export_plink('output/plink')

        will behave similarly to the PLINK VCF conversion command

        .. code-block:: text

            plink --vcf /path/to/file.vcf --make-bed --out sample --const-fid --keep-allele-order

        except:

        - The order among split multi-allelic alternatives in the BED
          file may disagree.
        - PLINK uses the rsID for the BIM file ID.

        :param str output: Output file base.  Will write BED, BIM, and FAM files.

        :param str fam_expr: Expression for FAM file fields.
        """

        self._jvdf.exportPlink(output, fam_expr)

    @handle_py4j
    def export_samples(self, output, expr, types=False):
        """Export sample information to delimited text file.

        **Examples**

        Export some sample QC metrics:

        >>> (vds.sample_qc()
        ...     .export_samples('output/samples.tsv', 'SAMPLE = s, CALL_RATE = sa.qc.callRate, NHET = sa.qc.nHet'))

        This will produce a file with a header and three columns.  To
        produce a file with no header, just leave off the assignment
        to the column identifier:

        >>> (vds.sample_qc()
        ...     .export_samples('output/samples.tsv', 's, sa.qc.rTiTv'))

        **Notes**

        One line per sample will be exported.  As :py:meth:`~hail.VariantDataset.export_samples` runs in sample context, the following symbols are in scope:

        - ``s`` (*Sample*): :ref:`sample`
        - ``sa``: sample annotations
        - ``global``: global annotations
        - ``gs`` (*Aggregable[Genotype]*): aggregable of :ref:`genotype` for sample ``s``

        :param str output: Output file.

        :param str expr: Export expression for values to export.

        :param bool types: Write types of exported columns to a file at (output + ".types").
        """

        self._jvds.exportSamples(output, expr, types)

    @handle_py4j
    def export_variants(self, output, expr, types=False):
        """Export variant information to delimited text file.

        **Examples**

        Export a four column TSV with ``v``, ``va.pass``, ``va.filters``, and
        one computed field: ``1 - va.qc.callRate``.

        >>> vds.export_variants('output/file.tsv',
        ...        'VARIANT = v, PASS = va.pass, FILTERS = va.filters, MISSINGNESS = 1 - va.qc.callRate')

        It is also possible to export without identifiers, which will result in
        a file with no header. In this case, the expressions should look like
        the examples below:

        >>> vds.export_variants('output/file.tsv', 'v, va.pass, va.qc.AF')

        .. note::

            If any field is named, all fields must be named.

        In the common case that a group of annotations needs to be exported (for
        example, the annotations produced by ``variantqc``), one can use the
        ``struct.*`` syntax.  This syntax produces one column per field in the
        struct, and names them according to the struct field name.

        For example, the following invocation (assuming ``va.qc`` was generated
        by :py:meth:`.variant_qc`):

        >>> vds.export_variants('output/file.tsv', 'variant = v, va.qc.*')

        will produce the following set of columns:

        .. code-block:: text

            variant  callRate  AC  AF  nCalled  ...

        Note that using the ``.*`` syntax always results in named arguments, so it
        is not possible to export header-less files in this manner.  However,
        naming the "splatted" struct will apply the name in front of each column
        like so:

        >>> vds.export_variants('output/file.tsv', 'variant = v, QC = va.qc.*')

        which produces these columns:

        .. code-block:: text

            variant  QC.callRate  QC.AC  QC.AF  QC.nCalled  ...


        **Notes**

        This module takes a comma-delimited list of fields or expressions to
        print. These fields will be printed in the order they appear in the
        expression in the header and on each line.

        One line per variant in the VDS will be printed.  The accessible namespace includes:

        - ``v`` (*Variant*): :ref:`variant`
        - ``va``: variant annotations
        - ``global``: global annotations
        - ``gs`` (*Aggregable[Genotype]*): aggregable of :ref:`genotype` for variant ``v``

        **Designating output with an expression**

        Much like the filtering methods, exporting allows flexible expressions
        to be written on the command line. While the filtering methods expect an
        expression that evaluates to true or false, this method expects a
        comma-separated list of fields to print. These fields *must* take the
        form ``IDENTIFIER = <expression>``.


        :param str output: Output file.

        :param str expr: Export expression for values to export.

        :param bool types: Write types of exported columns to a file at (output + ".types")
        """

        self._jvds.exportVariants(output, expr, types)

    @handle_py4j
    def export_variants_cass(self, variant_expr, genotype_expr,
                             address,
                             keyspace,
                             table,
                             export_missing=False,
                             export_ref=False,
                             drop=False,
                             block_size=100):
        """Export variant information to Cassandra."""

        self._jvdf.exportVariantsCassandra(address, genotype_expr, keyspace, table, variant_expr,
                                           drop, export_ref, export_missing, block_size)

    @handle_py4j
    def export_variants_solr(self, variant_expr, genotype_expr,
                             solr_url=None,
                             solr_cloud_collection=None,
                             zookeeper_host=None,
                             drop=False,
                             num_shards=1,
                             export_missing=False,
                             export_ref=False,
                             block_size=100):
        """Export variant information to Solr."""

        self._jvdf.exportVariantsSolr(variant_expr, genotype_expr, solr_cloud_collection, solr_url, zookeeper_host,
                                      export_missing, export_ref, drop, num_shards, block_size)

    @handle_py4j
    def export_vcf(self, output, append_to_header=None, export_pp=False, parallel=False):
        """Export variant dataset as a .vcf or .vcf.bgz file.

        **Examples**

        Export to VCF as a block-compressed file:

        >>> vds.export_vcf('output/example.vcf.bgz')

        **Notes**

        :py:meth:`~hail.VariantDataset.export_vcf` writes the VDS to disk in VCF format as described in the `VCF 4.2 spec <https://samtools.github.io/hts-specs/VCFv4.2.pdf>`_.

        Use the ``.vcf.bgz`` extension rather than ``.vcf`` in the output file name for `blocked GZIP <http://www.htslib.org/doc/tabix.html>`_ compression.

        .. note::

            We strongly recommended compressed (``.bgz`` extension) and parallel output (``parallel=True``) when exporting large VCFs.

        Consider the workflow of importing VCF to VDS and immediately exporting VDS to VCF:

        >>> vds.export_vcf('output/example_out.vcf')

        The *example_out.vcf* header will contain the FORMAT, FILTER, and INFO lines present in *example.vcf*. However, it will *not* contain CONTIG lines or lines added by external tools (such as bcftools and GATK) unless they are explicitly inserted using the ``append_to_header`` option.

        Hail only exports the contents of ``va.info`` to the INFO field. No other annotations besides ``va.info`` are exported.

        .. caution::

            If samples or genotypes are filtered after import, the value stored in ``va.info.AC`` value may no longer reflect the number of called alternate alleles in the filtered VDS. If the filtered VDS is then exported to VCF, downstream tools may produce erroneous results. The solution is to create new annotations in ``va.info`` or overwrite existing annotations. For example, in order to produce an accurate ``AC`` field, one can run :py:meth:`~hail.VariantDataset.variant_qc` and copy the ``va.qc.AC`` field to ``va.info.AC``:

            >>> (vds.filter_genotypes('g.gq >= 20')
            ...     .variant_qc()
            ...     .annotate_variants_expr('va.info.AC = va.qc.AC')
            ...     .export_vcf('output/example.vcf.bgz'))

        :param str output: Path of .vcf file to write.

        :param append_to_header: Path of file to append to VCF header.
        :type append_to_header: str or None

        :param bool export_pp: If True, export linear-scaled probabilities (Hail's `pp` field on genotype) as the VCF PP FORMAT field.

        :param bool parallel: If True, return a set of VCF files (one per partition) rather than serially concatenating these files.
        """

        self._jvdf.exportVCF(output, joption(append_to_header), export_pp, parallel)

    @handle_py4j
    def write(self, output, overwrite=False, parquet_genotypes=False):
        """Write variant dataset as VDS file.

        **Examples**

        Import data from a VCF file and then write the data to a VDS file:

        >>> vds.write("output/sample.vds")

        :param str output: Path of VDS file to write.

        :param bool overwrite: If True, overwrite any existing VDS file. Cannot be used to read from and write to the same path.

        :param bool parquet_genotypes: If True, store genotypes as Parquet rather than Hail's serialization.  The resulting VDS will be larger and slower in Hail but the genotypes will be accessible from other tools that support Parquet.

        """

        self._jvdf.write(output, overwrite, parquet_genotypes)

    @handle_py4j
    def filter_alleles(self, condition, annotation='va = va', subset=True, keep=True,
                       filter_altered_genotypes=False, max_shift=100):
        """Filter a user-defined set of alternate alleles for each variant.
        If all alternate alleles of a variant are filtered, the
        variant itself is filtered.  The condition expression is
        evaluated for each alternate allele, but not for
        the reference allele (i.e. ``aIndex`` will never be zero).

        **Examples**

        To remove alternate alleles with zero allele count and
        update the alternate allele count annotation with the new
        indices:

        >>> vds_result = vds.filter_alleles('va.info.AC[aIndex - 1] == 0',
        ...     annotation='va.info.AC = aIndices[1:].map(i => va.info.AC[i - 1])',
        ...     keep=False)

        Note that we skip the first element of ``aIndices`` because
        we are mapping between the old and new *allele* indices, not
        the *alternate allele* indices.

        **Notes**

        If ``filter_altered_genotypes`` is true, genotypes that contain filtered-out alleles are set to missing.

        :py:meth:`~hail.VariantDataset.filter_alleles` implements two algorithms for filtering alleles: subset and downcode. We will illustrate their
        behavior on the example genotype below when filtering the first alternate allele (allele 1) at a site with 1 reference
        allele and 2 alternate alleles.

        .. code-block:: text

          GT: 1/2
          GQ: 10
          AD: 0,50,35

          0 | 1000
          1 | 1000   10
          2 | 1000   0     20
            +-----------------
               0     1     2

        **Subset algorithm**

        The subset algorithm (the default, ``subset=True``) subsets the
        AD and PL arrays (i.e. removes entries corresponding to filtered alleles)
        and then sets GT to the genotype with the minimum PL.  Note
        that if the genotype changes (as in the example), the PLs
        are re-normalized (shifted) so that the most likely genotype has a PL of
        0.  Qualitatively, subsetting corresponds to the belief
        that the filtered alleles are not real so we should discard any
        probability mass associated with them.

        The subset algorithm would produce the following:

        .. code-block:: text

          GT: 1/1
          GQ: 980
          AD: 0,50

          0 | 980
          1 | 980    0
            +-----------
               0      1

        In summary:

        - GT: Set to most likely genotype based on the PLs ignoring the filtered allele(s).
        - AD: The filtered alleles' columns are eliminated, e.g., filtering alleles 1 and 2 transforms ``25,5,10,20`` to ``25,20``.
        - DP: No change.
        - PL: The filtered alleles' columns are eliminated and the remaining columns shifted so the minimum value is 0.
        - GQ: The second-lowest PL (after shifting).

        **Downcode algorithm**

        The downcode algorithm (``subset=False``) recodes occurances of filtered alleles
        to occurances of the reference allele (e.g. 1 -> 0 in our example). So the depths of filtered alleles in the AD field
        are added to the depth of the reference allele. Where downcodeing filtered alleles merges distinct genotypes, the minimum PL is used (since PL is on a log scale, this roughly corresponds to adding probabilities). The PLs
        are then re-normalized (shifted) so that the most likely genotype has a PL of 0, and GT is set to this genotype.
        If an allele is filtered, this algorithm acts similarly to :py:meth:`~hail.VariantDataset.split_multi`.

        The downcoding algorithm would produce the following:

        .. code-block:: text

          GT: 0/1
          GQ: 10
          AD: 35,50

          0 | 20
          1 | 0    10
            +-----------
              0    1

        In summary:

        - GT: Downcode filtered alleles to reference.
        - AD: The filtered alleles' columns are eliminated and their value is added to the reference, e.g., filtering alleles 1 and 2 transforms ``25,5,10,20`` to ``40,20``.
        - DP: No change.
        - PL: Downcode filtered alleles to reference, combine PLs using minimum for each overloaded genotype, and shift so the overall minimum PL is 0.
        - GQ: The second-lowest PL (after shifting).

        **Expression Variables**

        The following symbols are in scope for ``condition``:

        - ``v`` (*Variant*): :ref:`variant`
        - ``va``: variant annotations
        - ``aIndex`` (*Int*): the index of the allele being tested

        The following symbols are in scope for ``annotation``:

        - ``v`` (*Variant*): :ref:`variant`
        - ``va``: variant annotations
        - ``aIndices`` (*Array[Int]*): the array of old indices (such that ``aIndices[newIndex] = oldIndex`` and ``aIndices[0] = 0``)

        :param str condition: Filter expression involving v (variant), va (variant annotations), and aIndex (allele index)

        :param str annotation: Annotation modifying expression involving v (new variant), va (old variant annotations),
            and aIndices (maps from new to old indices)

        :param bool subset: If true, subsets PL and AD, otherwise downcodes the PL and AD.
            Genotype and GQ are set based on the resulting PLs.

        :param bool keep: If true, keep variants matching condition

        :param bool filter_altered_genotypes: If true, genotypes that contain filtered-out alleles are set to missing.

        :param int max_shift: maximum number of base pairs by which
            a split variant can move.  Affects memory usage, and will
            cause Hail to throw an error if a variant that moves further
            is encountered.

        :return: Filtered variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvdf.filterAlleles(condition, annotation, filter_altered_genotypes, keep, subset, max_shift)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def filter_genotypes(self, condition, keep=True):
        """Filter genotypes based on expression.

        **Examples**

        Filter genotypes by allele balance dependent on genotype call:

        >>> vds_result = vds.filter_genotypes('let ab = g.ad[1] / g.ad.sum() in ' +
        ...                      '((g.isHomRef() && ab <= 0.1) || ' +
        ...                      '(g.isHet() && ab >= 0.25 && ab <= 0.75) || ' +
        ...                      '(g.isHomVar() && ab >= 0.9))')

        **Notes**

        ``condition`` is in genotype context so the following symbols are in scope:

        - ``s`` (*Sample*): :ref:`sample`
        - ``v`` (*Variant*): :ref:`variant`
        - ``sa``: sample annotations
        - ``va``: variant annotations
        - ``global``: global annotations

        For more information, see the documentation on `data representation, annotations <overview.html#>`_, and
        the `expression language <exprlang.html>`_.

        .. caution::
            When ``condition`` evaluates to missing, the genotype will be removed regardless of whether ``keep=True`` or ``keep=False``.

        :param condition: Expression for filter condition.
        :type condition: str

        :return: Filtered variant dataset.
        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvdf.filterGenotypes(condition, keep)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def filter_multi(self):
        """Filter out multi-allelic sites.

        This method is much less computationally expensive than
        :py:meth:`.split_multi`, and can also be used to produce
        a variant dataset that can be used with methods that do not
        support multiallelic variants.

        :return: Dataset with no multiallelic sites, which can
            be used for biallelic-only methods.
        :rtype: :class:`.VariantDataset`
        """

        return VariantDataset(self.hc, self._jvdf.filterMulti())

    @handle_py4j
    def drop_samples(self):
        """Removes all samples from variant dataset.

        The variants, variant annotations, and global annnotations will remain,
        producing a sites-only variant dataset.

        :return: Sites-only variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        return VariantDataset(self.hc, self._jvds.dropSamples())

    @handle_py4j
    def filter_samples_expr(self, condition, keep=True):
        """Filter samples based on expression.

        **Examples**

        Filter samples by phenotype (assumes sample annotation *sa.isCase* exists and is a Boolean variable):

        >>> vds_result = vds.filter_samples_expr("sa.isCase")

        Remove samples with an ID that matches a regular expression:

        >>> vds_result = vds.filter_samples_expr('"^NA" ~ s.id' , keep=False)

        Filter samples from sample QC metrics and write output to a new variant dataset:

        >>> (vds.sample_qc()
        ...     .filter_samples_expr('sa.qc.callRate >= 0.99 && sa.qc.dpMean >= 10')
        ...     .write("output/filter_samples.vds"))

        **Notes**

        ``condition`` is in sample context so the following symbols are in scope:

        - ``s`` (*Sample*): :ref:`sample`
        - ``sa``: sample annotations
        - ``global``: global annotations
        - ``gs`` (*Aggregable[Genotype]*): aggregable of :ref:`genotype` for sample ``s``

        For more information, see the documentation on `data representation, annotations <overview.html#>`_, and
        the `expression language <exprlang.html>`_.

        .. caution::
            When ``condition`` evaluates to missing, the sample will be removed regardless of whether ``keep=True`` or ``keep=False``.


        :param condition: Expression for filter condition.
        :type condition: str

        :return: Filtered variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvds.filterSamplesExpr(condition, keep)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def filter_samples_list(self, input, keep=True):
        """Filter samples with a sample list file.

        **Examples**

        >>> vds_result = vds.filter_samples_list('data/exclude_samples.txt', keep=False)

        The file at the path ``input`` should contain on sample per
        line with no header or other fields.

        :param str input: Path to sample list file.

        :return: Filtered variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvds.filterSamplesList(input, keep)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def drop_variants(self):
        """Discard all variants, variant annotations and genotypes.

        Samples, sample annotations and global annotations are retained. This
        is the same as :func:`filter_variants_expr('false') <hail.VariantDataset.filter_variants_expr>`, but much faster.

        **Examples**

        >>> vds_result = vds.drop_variants()

        :return: Samples-only variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        return VariantDataset(self.hc, self._jvds.dropVariants())

    @handle_py4j
    def filter_variants_expr(self, condition, keep=True):
        """Filter variants based on expression.

        **Examples**

        Keep variants in the gene CHD8 (assumes the variant annotation ``va.gene`` exists):

        >>> vds_result = vds.filter_variants_expr('va.gene == "CHD8"')


        Remove all variants on chromosome 1:

        >>> vds_result = vds.filter_variants_expr('v.contig == "1"', keep=False)

        .. caution::

           The double quotes on ``"1"`` are necessary because ``v.contig`` is of type String.

        **Notes**

        The following symbols are in scope for ``condition``:

        - ``v`` (*Variant*): :ref:`variant`
        - ``va``: variant annotations
        - ``global``: global annotations
        - ``gs`` (*Aggregable[Genotype]*): aggregable of :ref:`genotype` for variant ``v``

        For more information, see the `Overview <overview.html#>`_ and the `Expression Language <exprlang.html>`_.

        .. caution::
           When ``condition`` evaluates to missing, the variant will be removed regardless of whether ``keep=True`` or ``keep=False``.

        :param condition: Expression for filter condition.
        :type condition: str

        :return: Filtered variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvds.filterVariantsExpr(condition, keep)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def filter_variants_intervals(self, intervals, keep=True):
        """Filter variants with an interval or interval tree.

        **Examples**

        >>> from hail.representation import *
        >>> vds_result = vds.filter_variants_intervals(IntervalTree.read('data/intervals.txt'))
        >>> vds_result = vds.filter_variants_intervals(IntervalTree.parse_all(['1:50M-75M', '2:START-400000','3-22']))
        >>> vds_result = vds.filter_variants_intervals(Interval(Locus('17', 38449840), Locus('17', 38530994)))
        >>> vds_result = vds.filter_variants_intervals(Interval.parse('17:38449840-38530994'))

        This method takes an argument either of :class:`.Interval` or :class:`.IntervalTree`.

        Based on the ``keep`` argument, this method will either restrict to variants in the
        supplied interval range, or remove all variants in that range.  Note that intervals
        are left-inclusive, and right-exclusive.  The below interval includes the locus
        ``15:100000`` but not ``15:101000``.

        >>> interval = Interval.parse('15:100000-101000')

        To supply a file containing intervals, use :py:meth:`.IntervalTree.read`:

        >>> vds_result = vds.filter_variants_intervals(IntervalTree.read('data/intervals.txt'))

        This method performs predicate pushdown when ``keep`` is ``True``, meaning that data shards
        that don't overlap any supplied interval will not be loaded at all.  This property
        enables ``filter_variants_intervals`` to be used for reasonably low-latency queries of one
        or more variants, even on large variant datasets:

        >>>  # We are interested in the variant 15:100203:A:T
        >>> vds_filtered = vds.filter_variants_expr('v.contig == "15" && v.start == 100203')  # slow
        >>> vds_filtered = vds.filter_variants_intervals(Interval.parse('15:100203-100204'))  # very fast

        :param intervals: interval or interval tree object
        :type intervals: s:class:`.Interval` or :class:`.IntervalTree`

        :return: Filtered variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        if isinstance(intervals, Interval):
            intervals = IntervalTree([intervals])
        elif not isinstance(intervals, IntervalTree):
            raise TypeError("argument 'intervals' must be of type Interval or IntervalTree, but found '%s'" %
                            type(intervals))

        jvds = self._jvds.filterIntervals(intervals._jrep, keep)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def filter_variants_list(self, input, keep=True):
        """Filter variants with a list of variants.

        **Examples**

        Keep all variants that occur in *data/variants.txt* (removing all other
        variants):

        >>> vds_result = vds.filter_variants_list('data/variants.txt')

        Remove all variants that occur in *data/variants.txt*:

        >>> vds_result = vds.filter_variants_list('data/variants.txt', keep=False)

        **File Format**

        Hail expects the given file to contain a variant per line following
        format: ``contig:pos:ref:alt1,alt2,...,altN``.

        :param str input: Path to variant list file.

        :return: Filtered variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvds.filterVariantsList(input, keep)
        return VariantDataset(self.hc, jvds)

    @property
    def globals(self):
        """Return global annotations as a Python object.

        :return: Dataset global annotations.
        :rtype: :py:class:`~hail.representation.Struct`
        """
        if self._globals is None:
            self._globals = self.global_schema._convert_to_py(self._jvds.globalAnnotation())
        return self._globals

    @handle_py4j
    def grm(self, output, format, id_file=None, n_file=None):
        """Compute the Genetic Relatedness Matrix (GRM).
        
        **Examples**
        
        >>> vds.grm('data/grm.rel', 'rel')
        
        **Notes**
        
        The genetic relationship matrix (GRM) :math:`G` encodes genetic correlation between each pair of samples. It is defined by :math:`G = MM^T` where :math:`M` is a standardized version of the genotype matrix, computed as follows. Let :math:`C` be the :math:`n \\times m` matrix of raw genotypes in the variant dataset, with rows indexed by :math:`n` samples and columns indexed by :math:`m` bialellic autosomal variants; :math:`C_{ij}` is the number of alternate alleles of variant :math:`j` carried by sample :math:`i`, which can be 0, 1, 2, or missing. For each variant :math:`j`, the sample alternate allele frequency :math:`p_j` is computed as half the mean of the non-missing entries of column :math:`j`. Entries of :math:`M` are then mean-centered and variance-normalized as

        .. math::

          M_{ij} = \\frac{C_{ij}-2p_j}{\sqrt{2p_j(1-p_j)m}},

        with :math:`M_{ij} = 0` for :math:`C_{ij}` missing (i.e. mean genotype imputation). This scaling normalizes genotype variances to a common value :math:`1/m` for variants in Hardy-Weinberg equilibrium and is further motivated in the paper `Patterson, Price and Reich, 2006 <http://journals.plos.org/plosgenetics/article?id=10.1371/journal.pgen.0020190>`_. (The resulting amplification of signal from the low end of the allele frequency spectrum will also introduce noise for rare variants; common practice is to filter out variants with minor allele frequency below some cutoff.)  The factor :math:`1/m` gives each sample row approximately unit total variance (assuming linkage equilibrium) so that the diagonal entries of the GRM are approximately 1. Equivalently,
        
        .. math::

          G_{ik} = \\frac{1}{m} \\sum_{j=1}^m \\frac{(C_{ij}-2p_j)(C_{kj}-2p_j)}{2 p_j (1-p_j)}
        
        The output formats are consistent with `PLINK formats <https://www.cog-genomics.org/plink2/formats>`_ as created by the `make-rel and make-grm commands <https://www.cog-genomics.org/plink2/distance#make_rel>`_ and used by `GCTA <http://cnsgenomics.com/software/gcta/estimate_grm.html>`_.

        :param str output: Output file.

        :param str format: Output format.  One of: 'rel', 'gcta-grm', 'gcta-grm-bin'.

        :param str id_file: ID file.

        :param str n_file: N file, for gcta-grm-bin only.
        """

        jvds = self._jvdf.grm(output, format, joption(id_file), joption(n_file))
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def hardcalls(self):
        """Drop all genotype fields except the GT field.

        A hard-called variant dataset is about two orders of magnitude
        smaller than a standard sequencing dataset. Use this
        method to create a smaller, faster
        representation for downstream processing that only
        requires the GT field.

        :return: Variant dataset with no genotype metadata.
        :rtype: :py:class:`.VariantDataset`
        """

        return VariantDataset(self.hc, self._jvdf.hardCalls())

    @handle_py4j
    def ibd(self, maf=None, bounded=True, min=None, max=None):
        """Compute matrix of identity-by-descent estimations.

        **Examples**

        To calculate a full IBD matrix, using minor allele frequencies computed
        from the variant dataset itself:

        >>> vds.ibd()

        To calculate an IBD matrix containing only pairs of samples with
        ``pi_hat`` in [0.2, 0.9], using minor allele frequencies stored in
        ``va.panel_maf``:

        >>> vds.ibd(maf='va.panel_maf', min=0.2, max=0.9)

        **Notes**

        The implementation is based on the IBD algorithm described in the `PLINK
        paper <http://www.ncbi.nlm.nih.gov/pmc/articles/PMC1950838>`_.

        :py:meth:`~hail.VariantDataset.ibd` requires the dataset to be
        bi-allelic (otherwise run :py:meth:`~hail.VariantDataset.split_multi` or otherwise run :py:meth:`~hail.VariantDataset.filter_multi`)
        and does not perform LD pruning. Linkage disequilibrium may bias the
        result so consider filtering variants first.

        The resulting :py:class:`.KeyTable` entries have the type: *{ i: String,
        j: String, ibd: { Z0: Double, Z1: Double, Z2: Double, PI_HAT: Double },
        ibs0: Long, ibs1: Long, ibs2: Long }*. The key list is: `*i: String, j:
        String*`.

        Conceptually, the output is a symmetric, sample-by-sample matrix. The
        output key table has the following form

        .. code-block: text

            i		j	ibd.Z0	ibd.Z1	ibd.Z2	ibd.PI_HAT ibs0	ibs1	ibs2
            sample1	sample2	1.0000	0.0000	0.0000	0.0000 ...
            sample1	sample3	1.0000	0.0000	0.0000	0.0000 ...
            sample1	sample4	0.6807	0.0000	0.3193	0.3193 ...
            sample1	sample5	0.1966	0.0000	0.8034	0.8034 ...

        :param maf: Expression for the minor allele frequency.
        :type maf: str or None

        :param bool bounded: Forces the estimations for Z0, Z1, Z2,
            and PI_HAT to take on biologically meaningful values
            (in the range [0,1]).

        :param min: Sample pairs with a PI_HAT below this value will
            not be included in the output. Must be in [0,1].
        :type min: float or None

        :param max: Sample pairs with a PI_HAT above this value will
            not be included in the output. Must be in [0,1].
        :type max: float or None

        :return: A :py:class:`.KeyTable` mapping pairs of samples to their IBD
            statistics

        :rtype: :py:class:`.KeyTable`

        """

        return KeyTable(self.hc, self._jvdf.ibd(joption(maf), bounded, joption(min), joption(max)))

    @handle_py4j
    def ibd_prune(self, threshold, maf=None, bounded=True):
        """
        Prune samples from variant dataset based on PI_HAT values of IBD computation.

        **Examples**

        Prune samples so that no two have a PI_HAT value greater than or equal to 0.4:

        >>> vds.ibd_prune(0.4)

        **Notes**

        The variant dataset returned may change in near future as a result of algorithmic improvements. The current algorithm is very efficient on datasets with many small
        families, less so on datasets with large families.

        :param threshold: The desired maximum PI_HAT value between any pair of samples.
        :param maf: Expression for the minor allele frequency.
        :param bounded: Forces the estimations for Z0, Z1, Z2, and PI_HAT to take on biologically meaningful values (in the range [0,1]).

        :return: A :py:class:`.VariantDataset` containing no samples with a PI_HAT greater than threshold.
        :rtype: :py:class:`.VariantDataset`
        """
        return VariantDataset(self.hc, self._jvdf.ibdPrune(threshold, joption(maf), bounded))

    @handle_py4j
    def impute_sex(self, maf_threshold=0.0, include_par=False, female_threshold=0.2, male_threshold=0.8, pop_freq=None):
        """Impute sex of samples by calculating inbreeding coefficient on the
        X chromosome.

        **Examples**

        Remove samples where imputed sex does not equal reported sex:

        >>> imputed_sex_vds = (vds.impute_sex()
        ...     .annotate_samples_expr('sa.sexcheck = sa.pheno.isFemale == sa.imputesex.isFemale')
        ...     .filter_samples_expr('sa.sexcheck || isMissing(sa.sexcheck)'))

        **Notes**

        We have used the same implementation as `PLINK v1.7 <http://pngu.mgh.harvard.edu/~purcell/plink/summary.shtml#sexcheck>`_.

        1. X chromosome variants are selected from the VDS: ``v.contig == "X" || v.contig == "23"``
        2. Variants with a minor allele frequency less than the threshold given by ``maf-threshold`` are removed
        3. Variants in the pseudoautosomal region `(X:60001-2699520) || (X:154931044-155260560)` are included if the ``include_par`` optional parameter is set to true.
        4. The minor allele frequency (maf) per variant is calculated.
        5. For each variant and sample with a non-missing genotype call, :math:`E`, the expected number of homozygotes (from population MAF), is computed as :math:`1.0 - (2.0*maf*(1.0-maf))`.
        6. For each variant and sample with a non-missing genotype call, :math:`O`, the observed number of homozygotes, is computed as `0 = heterozygote; 1 = homozygote`
        7. For each variant and sample with a non-missing genotype call, :math:`N` is incremented by 1
        8. For each sample, :math:`E`, :math:`O`, and :math:`N` are combined across variants
        9. :math:`F` is calculated by :math:`(O - E) / (N - E)`
        10. A sex is assigned to each sample with the following criteria: `F < 0.2 => Female; F > 0.8 => Male`. Use ``female-threshold`` and ``male-threshold`` to change this behavior.

        **Annotations**

        The below annotations can be accessed with ``sa.imputesex``.

        - **isFemale** (*Boolean*) -- True if the imputed sex is female, false if male, missing if undetermined
        - **Fstat** (*Double*) -- Inbreeding coefficient
        - **nTotal** (*Long*) -- Total number of variants considered
        - **nCalled**  (*Long*) -- Number of variants with a genotype call
        - **expectedHoms** (*Double*) -- Expected number of homozygotes
        - **observedHoms** (*Long*) -- Observed number of homozygotes


        :param float maf_threshold: Minimum minor allele frequency threshold.

        :param bool include_par: Include pseudoautosomal regions.

        :param float female_threshold: Samples are called females if F < femaleThreshold

        :param float male_threshold: Samples are called males if F > maleThreshold

        :param str pop_freq: Variant annotation for estimate of MAF.
            If None, MAF will be computed.

        :return: Annotated dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvdf.imputeSex(maf_threshold, include_par, female_threshold, male_threshold, joption(pop_freq))
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def join(self, right):
        """Join two variant datasets.

        **Notes**

        This method performs an inner join on variants,
        concatenates samples, and takes variant and
        global annotations from the left dataset (self).

        The datasets must have distinct samples, the same sample schema, and the same split status (both split or both multi-allelic).

        :param right: right-hand variant dataset
        :type right: :py:class:`.VariantDataset`

        :return: Joined variant dataset
        :rtype: :py:class:`.VariantDataset`
        """

        return VariantDataset(self.hc, self._jvds.join(right._jvds))

    @handle_py4j
    def linreg(self, y, covariates=[], root='va.linreg', min_ac=1, min_af=0.0):
        r"""Test each variant for association using linear regression.

        **Examples**

        Run linear regression per variant using a phenotype and two covariates stored in sample annotations:

        >>> vds_result = vds.linreg('sa.pheno.height', covariates=['sa.pheno.age', 'sa.pheno.isFemale'])

        **Notes**

        The :py:meth:`.linreg` command computes, for each variant, statistics of
        the :math:`t`-test for the genotype coefficient of the linear function
        of best fit from sample genotype and covariates to quantitative
        phenotype or case-control status. Hail only includes samples for which
        phenotype and all covariates are defined. For each variant, Hail imputes
        missing genotypes as the mean of called genotypes.

        Assuming there are sample annotations ``sa.pheno.height``,
        ``sa.pheno.age``, ``sa.pheno.isFemale``, and ``sa.cov.PC1``, the command:

        >>> vds_result = vds.linreg('sa.pheno.height', covariates=['sa.pheno.age', 'sa.pheno.isFemale', 'sa.cov.PC1'])

        considers a model of the form

        .. math::

            \mathrm{height} = \beta_0 + \beta_1 \, \mathrm{gt} + \beta_2 \, \mathrm{age} + \beta_3 \, \mathrm{isFemale} + \beta_4 \, \mathrm{PC1} + \varepsilon, \quad \varepsilon \sim \mathrm{N}(0, \sigma^2)

        where the genotype :math:`\mathrm{gt}` is coded as :math:`0` for HomRef, :math:`1` for
        Het, and :math:`2` for HomVar, and the Boolean covariate :math:`\mathrm{isFemale}`
        is coded as :math:`1` for true (female) and :math:`0` for false (male). The null
        model sets :math:`\beta_1 = 0`.

        :py:meth:`.linreg` skips variants that don't vary across the included samples,
        such as when all genotypes are homozygous reference. One can further
        restrict computation to those variants with at least :math:`k` observed
        alternate alleles (AC) or alternate allele frequency (AF) at least
        :math:`p` in the included samples using the options ``minac=k`` or
        ``minaf=p``, respectively. Unlike the :py:meth:`.filter_variants_expr`
        command, these filters do not remove variants from the underlying
        variant dataset. Adding both filters is equivalent to applying the more
        stringent of the two, as AF equals AC over twice the number of included
        samples.

        Phenotype and covariate sample annotations may also be specified using `programmatic expressions <exprlang.html>`_ without identifiers, such as:

        .. code-block:: text

          if (sa.isFemale) sa.cov.age else (2 * sa.cov.age + 10)

        For Boolean covariate types, true is coded as 1 and false as 0. In particular, for the sample annotation ``sa.fam.isCase`` added by importing a FAM file with case-control phenotype, case is 1 and control is 0.

        Hail's linear regression test corresponds to the ``q.lm`` test in
        `EPACTS <http://genome.sph.umich.edu/wiki/EPACTS#Single_Variant_Tests>`_. For
        each variant, Hail imputes missing genotypes as the mean of called
        genotypes, whereas EPACTS subsets to those samples with called
        genotypes. Hence, Hail and EPACTS results will currently only agree for
        variants with no missing genotypes.

        The standard least-squares linear regression model is derived in Section
        3.2 of `The Elements of Statistical Learning, 2nd Edition
        <http://statweb.stanford.edu/~tibs/ElemStatLearn/printings/ESLII_print10.pdf>`_. See
        equation 3.12 for the t-statistic which follows the t-distribution with
        :math:`n - k - 2` degrees of freedom, under the null hypothesis of no
        effect, with :math:`n` samples and :math:`k` covariates in addition to
        genotype and intercept.

        **Annotations**

        With the default root, the following four variant annotations are added.

        - **va.linreg.beta** (*Double*) -- fit genotype coefficient, :math:`\hat\beta_1`
        - **va.linreg.se** (*Double*) -- estimated standard error, :math:`\widehat{\mathrm{se}}`
        - **va.linreg.tstat** (*Double*) -- :math:`t`-statistic, equal to :math:`\hat\beta_1 / \widehat{\mathrm{se}}`
        - **va.linreg.pval** (*Double*) -- :math:`p`-value

        :param str y: Response expression

        :param covariates: list of covariate expressions
        :type covariates: list of str

        :param str root: Variant annotation path to store result of linear regression.

        :param int min_ac: Minimum alternate allele count.

        :param float min_af: Minimum alternate allele frequency.

        :return: Variant dataset with linear regression variant annotations.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvdf.linreg(y, jarray(Env.jvm().java.lang.String, covariates), root, min_ac, min_af)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def lmmreg(self, kinship_vds, y, covariates=[], global_root="global.lmmreg", va_root="va.lmmreg",
               run_assoc=True, use_ml=False, delta=None, sparsity_threshold=1.0, force_block=False,
               force_grammian=False):
        """Use a kinship-based linear mixed model to estimate the genetic component of phenotypic variance (narrow-sense heritability) and optionally test each variant for association.

        **Examples**

        Suppose the variant dataset saved at *data/example_lmmreg.vds* has a Boolean variant annotation ``va.useInKinship`` and numeric or Boolean sample annotations ``sa.pheno``, ``sa.cov1``, ``sa.cov2``. Then the :py:meth:`.lmmreg` function in

        >>> assoc_vds = hc.read("data/example_lmmreg.vds")
        >>> kinship_vds = assoc_vds.filter_variants_expr('va.useInKinship')
        >>> lmm_vds = assoc_vds.lmmreg(kinship_vds, 'sa.pheno', ['sa.cov1', 'sa.cov2'])

        will execute the following five steps in order:

        1) filter to samples for which ``sa.pheno``, ``sa.cov``, and ``sa.cov2`` are all defined
        2) compute the kinship matrix :math:`K` (the RRM defined below) using those variants in ``kinship_vds``
        3) compute the eigendecomposition :math:`K = USU^T` of the kinship matrix
        4) fit covariate coefficients and variance parameters in the sample-covariates-only (global) model using restricted maximum likelihood (`REML <https://en.wikipedia.org/wiki/Restricted_maximum_likelihood>`_), storing results in global annotations under ``global.lmmreg``
        5) test each variant for association, storing results under ``va.lmmreg`` in variant annotations

        This plan can be modified as follows:

        - Set ``run_assoc=False`` to not test any variants for association, i.e. skip Step 5.
        - Set ``use_ml=True`` to use maximum likelihood instead of REML in Steps 4 and 5.
        - Set the ``delta`` argument to manually set the value of :math:`\delta` rather that fitting :math:`\delta` in Step 4.
        - Set the ``global_root`` argument to change the global annotation root in Step 4.
        - Set the ``va_root`` argument to change the variant annotation root in Step 5.

        :py:meth:`.lmmreg` adds eight global annotations in Step 4; the last three are omitted if :math:`\delta` is set rather than fit.

        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | Annotation                         | Type                 | Value                                                                                                                                                |
        +====================================+======================+======================================================================================================================================================+
        | ``global.lmmreg.useML``            | Boolean              | true if fit by ML, false if fit by REML                                                                                                              |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | ``global.lmmreg.beta``             | Dict[String, Double] | map from *intercept* and the given ``covariates`` expressions to the corresponding fit :math:`\\beta` coefficients                                    |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | ``global.lmmreg.sigmaG2``          | Double               | fit coefficient of genetic variance, :math:`\\hat{\sigma}_g^2`                                                                                        |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | ``global.lmmreg.sigmaE2``          | Double               | fit coefficient of environmental variance :math:`\\hat{\sigma}_e^2`                                                                                   |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | ``global.lmmreg.delta``            | Double               | fit ratio of variance component coefficients, :math:`\\hat{\delta}`                                                                                   |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | ``global.lmmreg.h2``               | Double               | fit narrow-sense heritability, :math:`\\hat{h}^2`                                                                                                     |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | ``global.lmmreg.evals``            | Array[Double]        | eigenvalues of the kinship matrix in descending order                                                                                                |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | ``global.lmmreg.fit.logDeltaGrid`` | Array[Double]        | values of :math:`\\mathit{ln}(\delta)` used in the grid search                                                                                        |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | ``global.lmmreg.fit.logLkhdVals``  | Array[Double]        | (restricted) log likelihood of :math:`y` given :math:`X` and :math:`\\mathit{ln}(\delta)` at the (RE)ML fit of :math:`\\beta` and :math:`\sigma_g^2`   |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
        | ``global.lmmreg.fit.maxLogLkhd``   | Double               | (restricted) maximum log likelihood corresponding to the fit delta                                                                                   |
        +------------------------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+

        These global annotations are also added to ``hail.log``, with the ranked evals and :math:`\delta` grid with values in .tsv tabular form.  Use ``grep 'lmmreg:' hail.log`` to find the lines just above each table.

        If Step 5 is performed, :py:meth:`.lmmreg` also adds nine variant annotations.

        +------------------------+--------+-------------------------------------------------------------------------+
        | Annotation             | Type   | Value                                                                   |
        +========================+========+=========================================================================+
        | ``va.lmmreg.beta``     | Double | fit genotype coefficient, :math:`\hat\\beta_0`                           |
        +------------------------+--------+-------------------------------------------------------------------------+
        | ``va.lmmreg.sigmaG2``  | Double | fit coefficient of genetic variance component, :math:`\hat{\sigma}_g^2` |
        +------------------------+--------+-------------------------------------------------------------------------+
        | ``va.lmmreg.chi2``     | Double | :math:`\chi^2` statistic of the likelihood ratio test                   |
        +------------------------+--------+-------------------------------------------------------------------------+
        | ``va.lmmreg.pval``     | Double | :math:`p`-value                                                         |
        +------------------------+--------+-------------------------------------------------------------------------+
        | ``va.lmmreg.AF``       | Double | minor allele frequency for included samples                             |
        +------------------------+--------+-------------------------------------------------------------------------+
        | ``va.lmmreg.nHomRef``  | Int    | count of HomRef genotypes for included samples                          |
        +------------------------+--------+-------------------------------------------------------------------------+
        | ``va.lmmreg.nHet``     | Int    | count of Het genotypes among for samples                                |
        +------------------------+--------+-------------------------------------------------------------------------+
        | ``va.lmmreg.nHomVar``  | Int    | count of HomVar genotypes for included samples                          |
        +------------------------+--------+-------------------------------------------------------------------------+
        | ``va.lmmreg.nMissing`` | Int    | count of missing genotypes for included samples                         |
        +------------------------+--------+-------------------------------------------------------------------------+

        Those variants that don't vary across the included samples (e.g., all genotypes are HomRef) will have missing annotations.

        The simplest way to export all resulting annotations is:

        >>> lmm_vds.export_variants('output/lmmreg.tsv.bgz', 'variant = v, va.lmmreg.*')
        >>> lmmreg_results = lmm_vds.globals['lmmreg']

        **Performance**

        Hail's initial version of :py:meth:`.lmmreg` scales to well beyond 10k samples and to an essentially unbounded number of variants, making it particularly well-suited to modern sequencing studies and complementary to tools designed for SNP arrays. The first analysts to apply :py:meth:`.lmmreg` in research computed kinship from 262k common variants and tested 25 million non-rare variants on 8185 whole genomes in 32 minutes. As another example, starting from a VDS of the 1000 Genomes Project (consisting of 2535 whole genomes), :py:meth:`.lmmreg` computes a kinship matrix based on 100k common variants, fits coefficients and variance components in the sample-covariates-only model, runs a linear-mixed-model likelihood ratio test for all 15 million high-quality non-rare variants, and exports the results in 3m42s minutes. Here we used 42 preemptible workers (~680 cores) on 2k partitions at a compute cost of about 50 cents on Google cloud (see `Using Hail on the Google Cloud Platform <http://discuss.hail.is/t/using-hail-on-the-google-cloud-platform/80>`_).

        While :py:meth:`.lmmreg` computes the kinship matrix :math:`K` using distributed matrix multiplication (Step 2), the full eigendecomposition (Step 3) is currently run on a single core of master using the `LAPACK routine DSYEVD <http://www.netlib.org/lapack/explore-html/d2/d8a/group__double_s_yeigen_ga694ddc6e5527b6223748e3462013d867.html>`_, which we empirically find to be the most performant of the four available routines; laptop performance plots showing cubic complexity in :math:`n` are available `here <https://github.com/hail-is/hail/pull/906>`_. On Google cloud, eigendecomposition takes about 2 seconds for 2535 sampes and 1 minute for 8185 samples. If you see worse performance, check that LAPACK natives are being properly loaded (see "BLAS and LAPACK" in Getting Started).

        Given the eigendecomposition, fitting the global model (Step 4) takes on the order of a few seconds on master. Association testing (Step 5) is fully distributed by variant with per-variant time complexity that is completely independent of the number of sample covariates and dominated by multiplication of the genotype vector :math:`v` by the matrix of eigenvectors :math:`U^T` as described below, which we accelerate with a sparse representation of :math:`v`.  The matrix :math:`U^T` has size about :math:`8n^2` bytes and is currently broadcast to each Spark executor. For example, with 15k samples, storing :math:`U^T` consumes about 3.6GB of memory on a 16-core worker node with two 8-core executors. So for large :math:`n`, we recommend using a high-memory configuration such as ``highmem`` workers.

        **Linear mixed model**

        :py:meth:`.lmmreg` estimates the genetic proportion of residual phenotypic variance (narrow-sense heritability) under a kinship-based linear mixed model, and then optionally tests each variant for association using the likelihood ratio test. Inference is exact.

        We first describe the sample-covariates-only model used to estimate heritability, which we simply refer to as the *global model*. With :math:`n` samples and :math:`c` sample covariates, we define:

        - :math:`y = n \\times 1` vector of phenotypes
        - :math:`X = n \\times c` matrix of sample covariates and intercept column of ones
        - :math:`K = n \\times n` kinship matrix
        - :math:`I = n \\times n` identity matrix
        - :math:`\\beta = c \\times 1` vector of covariate coefficients
        - :math:`\sigma_g^2 =` coefficient of genetic variance component :math:`K`
        - :math:`\sigma_e^2 =` coefficient of environmental variance component :math:`I`
        - :math:`\delta = \\frac{\sigma_e^2}{\sigma_g^2} =` ratio of environmental and genetic variance component coefficients
        - :math:`h^2 = \\frac{\sigma_g^2}{\sigma_g^2 + \sigma_e^2} = \\frac{1}{1 + \delta} =` genetic proportion of residual phenotypic variance

        Under a linear mixed model, :math:`y` is sampled from the :math:`n`-dimensional `multivariate normal distribution <https://en.wikipedia.org/wiki/Multivariate_normal_distribution>`_ with mean :math:`X \\beta` and variance components that are scalar multiples of :math:`K` and :math:`I`:

        .. math::

          y \sim \mathrm{N}\\left(X\\beta, \sigma_g^2 K + \sigma_e^2 I\\right)

        Thus the model posits that the residuals :math:`y_i - X_{i,:}\\beta` and :math:`y_j - X_{j,:}\\beta` have covariance :math:`\sigma_g^2 K_{ij}` and approximate correlation :math:`h^2 K_{ij}`. Informally: phenotype residuals are correlated as the product of overall heritability and pairwise kinship. By contrast, standard (unmixed) linear regression is equivalent to fixing :math:`\sigma_2` (equivalently, :math:`h^2`) at 0 above, so that all phenotype residuals are independent.

        **Caution:** while it is tempting to interpret :math:`h^2` as the `narrow-sense heritability <https://en.wikipedia.org/wiki/Heritability#Definition>`_ of the phenotype alone, note that its value depends not only the phenotype and genetic data, but also on the choice of sample covariates.

        **Fitting the global model**

        The core algorithm is essentially a distributed implementation of the spectral approach taken in `FastLMM <https://www.microsoft.com/en-us/research/project/fastlmm/>`_. Let :math:`K = USU^T` be the `eigendecomposition <https://en.wikipedia.org/wiki/Eigendecomposition_of_a_matrix#Real_symmetric_matrices>`_ of the real symmetric matrix :math:`K`. That is:

        - :math:`U = n \\times n` orthonormal matrix whose columns are the eigenvectors of :math:`K`
        - :math:`S = n \\times n` diagonal matrix of eigenvalues of :math:`K` in descending order. :math:`S_{ii}` is the eigenvalue of eigenvector :math:`U_{:,i}`
        - :math:`U^T = n \\times n` orthonormal matrix, the transpose (and inverse) of :math:`U`

        A bit of matrix algebra on the multivariate normal density shows that the linear mixed model above is mathematically equivalent to the model

        .. math::

          U^Ty \\sim \mathrm{N}\\left(U^TX\\beta, \sigma_g^2 (S + \delta I)\\right)

        for which the covariance is diagonal (e.g., unmixed). That is, rotating the phenotype vector (:math:`y`) and covariate vectors (columns of :math:`X`) in :math:`\mathbb{R}^n` by :math:`U^T` transforms the model to one with independent residuals. For any particular value of :math:`\delta`, the restricted maximum likelihood (REML) solution for the latter model can be solved exactly in time complexity that is linear rather than cubic in :math:`n`.  In particular, having rotated, we can run a very efficient 1-dimensional optimization procedure over :math:`\delta` to find the REML estimate :math:`(\hat{\delta}, \\hat{\\beta}, \\hat{\sigma}_g^2)` of the triple :math:`(\delta, \\beta, \sigma_g^2)`, which in turn determines :math:`\\hat{\sigma}_e^2` and :math:`\\hat{h}^2`.

        We first compute the maximum log likelihood on a :math:`\delta`-grid that is uniform on the log scale, with :math:`\\mathit{ln}(\delta)` running from -10 to 10 by 0.01, corresponding to :math:`h^2` decreasing from 0.999999998 to 0.000000002. If :math:`h^2` is maximized at the lower boundary then standard linear regression would be more appropriate and Hail will exit; more generally, consider using standard linear regression when :math:`\\hat{h}^2` is very small. A maximum at the upper boundary is highly suspicious and will also cause Hail to exit, with the ``hail.log`` recording all values over the grid for further inspection.

        If the optimal grid point falls in the interior of the grid as expected, we then use `Brent's method <https://en.wikipedia.org/wiki/Brent%27s_method>`_ to find the precise location of the maximum over the same range, with initial guess given by the optimal grid point and a tolerance on :math:`\\mathit{ln}(\delta)` of 1e-6. If this location differs from the optimal grid point by more than .01, a warning will be displayed and logged, and one would be wise to investigate by plotting the values over the grid. Note that :math:`h^2` is related to :math:`\\mathit{ln}(\delta)` through the `sigmoid function <https://en.wikipedia.org/wiki/Sigmoid_function>`_. Hence one can change variables to extract a high-resolution discretization of the likelihood function of :math:`h^2` over :math:`[0,1]` at the corresponding REML estimators for :math:`\\beta` and :math:`\sigma_g^2`.

        **Testing each variant for association**

        Fixing a single variant, we define:

        - :math:`v = n \\times 1` vector of genotypes, with missing genotypes imputed as the mean of called genotypes
        - :math:`X_v = \\left[v | X \\right] = n \\times (1 + c)` matrix concatenating :math:`v` and :math:`X`
        - :math:`\\beta_v = (\\beta^0_v, \\beta^1_v, \\ldots, \\beta^c_v) = (1 + c) \\times 1` vector of covariate coefficients

        Fixing :math:`\delta` at the global REML estimate :math:`\\hat{\delta}`, we find the REML estimate :math:`(\\hat{\\beta}_v, \\hat{\sigma}_{g,v}^2)` via rotation of the model

        .. math::

          y \\sim \\mathrm{N}\\left(X_v\\beta_v, \sigma_{g,v}^2 (K + \\hat{\delta} I)\\right)

        Note that the only new rotation to compute here is :math:`U^T v`.

        To test the null hypothesis that the genotype coefficient :math:`\\beta^0_v` is zero, we consider the restricted model with parameters :math:`((0, \\beta^1_v, \ldots, \\beta^c_v), \sigma_{g,v}^2)` within the full model with parameters :math:`(\\beta^0_v, \\beta^1_v, \\ldots, \\beta^c_v), \sigma_{g_v}^2)`, with :math:`\delta` fixed at :math:`\\hat\delta` in both. The latter fit is simply that of the global model, :math:`((0, \\hat{\\beta}^1, \\ldots, \\hat{\\beta}^c), \\hat{\sigma}_g^2)`. The likelihood ratio test statistic is given by

        .. math::

          \chi^2 = n \\, \\mathit{ln}\left(\\frac{\hat{\sigma}^2_g}{\\hat{\sigma}_{g,v}^2}\\right)

        and follows a chi-squared distribution with one degree of freedom. Here the ratio :math:`\\hat{\sigma}^2_g / \\hat{\sigma}_{g,v}^2` captures the degree to which adding the variant :math:`v` to the global model reduces the residual phenotypic variance.

        **Kinship: Realized Relationship Matrix (RRM)**

        As in FastLMM, :py:meth:`.lmmreg` uses the Realized Relationship Matrix (RRM) for kinship, defined as follows. Consider the :math:`n \\times m` matrix :math:`C` of raw genotypes, with rows indexed by :math:`n` samples and columns indexed by the :math:`m` bialellic autosomal variants for which ``va.useInKinship`` is true; :math:`C_{ij}` is the number of alternate alleles of variant :math:`j` carried by sample :math:`i`, which can be 0, 1, 2, or missing. For each variant :math:`j`, the sample alternate allele frequency :math:`p_j` is computed as half the mean of the non-missing entries of column :math:`j`. Entries of :math:`M` are then mean-centered and variance-normalized as

        .. math::

          M_{ij} = \\frac{C_{ij}-2p_j}{\sqrt{\\frac{m}{n} \sum_{k=1}^n (C_{ij}-2p_j)^2}},

        with :math:`M_{ij} = 0` for :math:`C_{ij}` missing (i.e. mean genotype imputation). This scaling normalizes each variant column to have empirical variance :math:`1/m`, which gives each sample row approximately unit total variance (assuming linkage equilibrium) and yields the :math:`n \\times n` sample correlation or realized relationship matrix (RRM) :math:`K` as simply

        .. math::

          K = MM^T

        Note that the only difference between the Realized Relationship Matrix and the Genetic Relationship Matrix (GRM) used in :py:meth:`~hail.VariantDataset.pca` is the variant (column) normalization: where RRM uses empirical variance, GRM uses expected variance under Hardy-Weinberg Equilibrium.

        **Further background**

        For the history and mathematics of linear mixed models in genetics, including `FastLMM <https://www.microsoft.com/en-us/research/project/fastlmm/>`_, see `Christoph Lippert's PhD thesis <https://publikationen.uni-tuebingen.de/xmlui/bitstream/handle/10900/50003/pdf/thesis_komplett.pdf>`_. For an investigation of various approaches to defining kinship, see `Comparison of Methods to Account for Relatedness in Genome-Wide Association Studies with Family-Based Data <http://journals.plos.org/plosgenetics/article?id=10.1371/journal.pgen.1004445>`_.

        :param kinship_vds: Variant dataset used to compute kinship
        :type kinship_vds: :class:`.VariantDataset`

        :param str y: Response sample annotation.

        :param covariates: List of covariate sample annotations
        :type covariates: list of str

        :param str global_root: Global annotation root, a period-delimited path starting with `global`.

        :param str va_root: Variant annotation root, a period-delimited path starting with `va`.

        :param bool run_assoc: If True, run association testing in addition to fitting the global model

        :param bool use_ml: Use ML instead of REML throughout.

        :param delta: Fixed delta value to use in the global model, overrides fitting delta.
        :type delta: float or None

        :param float sparsity_threshold: AF threshold above which to use dense genotype vectors in rotation (advanced).

        :param bool force_block: Force using Spark's BlockMatrix to compute kinship (advanced).

        :param bool force_grammian: Force using Spark's RowMatrix.computeGrammian to compute kinship (advanced).

        :return: Variant dataset with linear mixed regression annotations
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvdf.lmmreg(kinship_vds._jvds, y, jarray(Env.jvm().java.lang.String, covariates),
                                 use_ml, global_root, va_root, run_assoc,
                                 joption(delta), sparsity_threshold, force_block, force_grammian)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def logreg(self, test, y, covariates=[], root='va.logreg'):
        """Test each variant for association using logistic regression.

        **Examples**

        Run the logistic regression Wald test per variant using a phenotype and two covariates stored in sample annotations:

        >>> vds_result = vds.logreg('wald', 'sa.pheno.isCase', covariates=['sa.pheno.age', 'sa.pheno.isFemale'])

        **Notes**

        The :py:meth:`~hail.VariantDataset.logreg` command performs,
        for each variant, a significance test of the genotype in
        predicting a binary (case-control) phenotype based on the
        logistic regression model. Hail supports the Wald test ('wald'),
        likelihood ratio test ('lrt'), Rao score test ('score'), and Firth test ('firth'). Hail only
        includes samples for which the phenotype and all covariates are
        defined. For each variant, Hail imputes missing genotypes as
        the mean of called genotypes.

        The example above considers a model of the form

        .. math::

          \mathrm{Prob}(\mathrm{isCase}) = \mathrm{sigmoid}(\\beta_0 + \\beta_1 \, \mathrm{gt} + \\beta_2 \, \mathrm{age} + \\beta_3 \, \mathrm{isFemale} + \\varepsilon), \quad \\varepsilon \sim \mathrm{N}(0, \sigma^2)

        where :math:`\mathrm{sigmoid}` is the `sigmoid
        function <https://en.wikipedia.org/wiki/Sigmoid_function>`_, the
        genotype :math:`\mathrm{gt}` is coded as 0 for HomRef, 1 for
        Het, and 2 for HomVar, and the Boolean covariate
        :math:`\mathrm{isFemale}` is coded as 1 for true (female) and
        0 for false (male). The null model sets :math:`\\beta_1 = 0`.

        The resulting variant annotations depend on the test statistic
        as shown in the tables below.

        ========== =================== ====== =====
        Test       Annotation          Type   Value
        ========== =================== ====== =====
        Wald       ``va.logreg.beta``  Double fit genotype coefficient, :math:`\hat\\beta_1`
        Wald       ``va.logreg.se``    Double estimated standard error, :math:`\widehat{\mathrm{se}}`
        Wald       ``va.logreg.zstat`` Double Wald :math:`z`-statistic, equal to :math:`\hat\\beta_1 / \widehat{\mathrm{se}}`
        Wald       ``va.logreg.pval``  Double Wald p-value testing :math:`\\beta_1 = 0`
        LRT, Firth ``va.logreg.beta``  Double fit genotype coefficient, :math:`\hat\\beta_1`
        LRT, Firth ``va.logreg.chi2``  Double deviance statistic
        LRT, Firth ``va.logreg.pval``  Double LRT / Firth p-value testing :math:`\\beta_1 = 0`
        Score      ``va.logreg.chi2``  Double score statistic
        Score      ``va.logreg.pval``  Double score p-value testing :math:`\\beta_1 = 0`
        ========== =================== ====== =====

        For the Wald and likelihood ratio tests, Hail fits the logistic model for each variant using Newton iteration and only emits the above annotations when the maximum likelihood estimate of the coefficients converges. The Firth test uses a modified form of Newton iteration. To help diagnose convergence issues, Hail also emits three variant annotations which summarize the iterative fitting process:

        ================ =========================== ======= =====
        Test             Annotation                  Type    Value
        ================ =========================== ======= =====
        Wald, LRT, Firth ``va.logreg.fit.nIter``     Int     number of iterations until convergence, explosion, or reaching the max (25 for Wald, LRT; 100 for Firth)
        Wald, LRT, Firth ``va.logreg.fit.converged`` Boolean true if iteration converged
        Wald, LRT, Firth ``va.logreg.fit.exploded``  Boolean true if iteration exploded
        ================ =========================== ======= =====

        We consider iteration to have converged when every coordinate of :math:`\\beta` changes by less than :math:`10^{-6}`. For Wald and LRT, up to 25 iterations are attempted; in testing we find 4 or 5 iterations nearly always suffice. Convergence may also fail due to explosion, which refers to low-level numerical linear algebra exceptions caused by manipulating ill-conditioned matrices. Explosion may result from (nearly) linearly dependent covariates or complete `separation <https://en.wikipedia.org/wiki/Separation_(statistics)>`_.

        A more common situation in genetics is quasi-complete seperation, e.g. variants that are observed only in cases (or controls). Such variants inevitably arise when testing millions of variants with very low minor allele count. The maximum likelihood estimate of :math:`\\beta` under logistic regression is then undefined but convergence may still occur after a large number of iterations due to a very flat likelihood surface. In testing, we find that such variants produce a secondary bump from 10 to 15 iterations in the histogram of number of iterations per variant. We also find that this faux convergence produces large standard errors and large (insignificant) p-values. To not miss such variants, consider using Firth logistic regression, linear regression, or group-based tests.

        Here's a concrete illustration of quasi-complete seperation in R. Suppose we have 2010 samples distributed as follows for a particular variant:

        ======= ====== === ======
        Status  HomRef Het HomVar
        ======= ====== === ======
        Case    1000   10  0
        Control 1000   0   0
        ======= ====== === ======

        The following R code fits the (standard) logistic, Firth logistic, and linear regression models to this data, where ``x`` is genotype, ``y`` is phenotype, and ``logistf`` is from the logistf package:

        .. code-block:: R

            x <- c(rep(0,1000), rep(1,1000), rep(1,10)
            y <- c(rep(0,1000), rep(0,1000), rep(1,10))
            logfit <- glm(y ~ x, family=binomial())
            firthfit <- logistf(y ~ x)
            linfit <- lm(y ~ x)

        The resulting p-values for the genotype coefficient are 0.991, 0.00085, and 0.0016, respectively. The erroneous value 0.991 is due to quasi-complete separation. Moving one of the 10 hets from case to control eliminates this quasi-complete separation; the p-values from R are then 0.0373, 0.0111, and 0.0116, respectively, as expected for a less significant association.

        The Firth test reduces bias from small counts and resolves the issue of separation by penalizing maximum likelihood estimation by the `Jeffrey's invariant prior <https://en.wikipedia.org/wiki/Jeffreys_prior>`_. This test is slower, as both the null and full model must be fit per variant, and convergence of the modified Newton method is linear rather than quadratic. For Firth, 100 iterations are attempted for the null model and, if that is successful, for the full model as well. In testing we find 20 iterations nearly always suffices. If the null model fails to converge, then the ``sa.lmmreg.fit`` annotations reflect the null model; otherwise, they reflect the full model.

        See `Recommended joint and meta-analysis strategies for case-control association testing of single low-count variants <http://www.ncbi.nlm.nih.gov/pmc/articles/PMC4049324/>`_ for an empirical comparison of the logistic Wald, LRT, score, and Firth tests. The theoretical foundations of the Wald, likelihood ratio, and score tests may be found in Chapter 3 of Gesine Reinert's notes `Statistical Theory <http://www.stats.ox.ac.uk/~reinert/stattheory/theoryshort09.pdf>`_.  Firth introduced his approach in `Bias reduction of maximum likelihood estimates, 1993 <http://www2.stat.duke.edu/~scs/Courses/Stat376/Papers/GibbsFieldEst/BiasReductionMLE.pdf>`_. Heinze and Schemper further analyze Firth's approach in `A solution to the problem of separation in logistic regression, 2002 <https://cemsiis.meduniwien.ac.at/fileadmin/msi_akim/CeMSIIS/KB/volltexte/Heinze_Schemper_2002_Statistics_in_Medicine.pdf>`_.

        Phenotype and covariate sample annotations may also be specified using `programmatic expressions <exprlang.html>`_ without identifiers, such as:

        .. code-block:: text

            if (sa.isFemale) sa.cov.age else (2 * sa.cov.age + 10)

        For Boolean covariate types, true is coded as 1 and false as 0. In particular, for the sample annotation ``sa.fam.isCase`` added by importing a FAM file with case-control phenotype, case is 1 and control is 0.

        Hail's logistic regression tests correspond to the ``b.wald``, ``b.lrt``, and ``b.score`` tests in `EPACTS <http://genome.sph.umich.edu/wiki/EPACTS#Single_Variant_Tests>`_. For each variant, Hail imputes missing genotypes as the mean of called genotypes, whereas EPACTS subsets to those samples with called genotypes. Hence, Hail and EPACTS results will currently only agree for variants with no missing genotypes.

        :param str test: Statistical test, one of: 'wald', 'lrt', 'score', or 'firth'.

        :param str y: Response expression.  Must evaluate to Boolean or
            numeric with all values 0 or 1.

        :param covariates: list of covariate expressions
        :type covariates: list of str

        :param str root: Variant annotation path to store result of linear regression.

        :return: Variant dataset with logistic regression variant annotations.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvdf.logreg(test, y, jarray(Env.jvm().java.lang.String, covariates), root)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def mendel_errors(self, output, fam):
        """Find Mendel errors; count per variant, individual and nuclear
        family.

        **Examples**

        Find all violations of Mendelian inheritance in each (dad,
        mom, kid) trio in *trios.fam* and save results to files with root ``mydata``:

        >>> vds_result = vds.mendel_errors('output/genomes', 'data/trios.fam')

        **Notes**

        The code above outputs four TSV files according to the `PLINK mendel
        formats <https://www.cog-genomics.org/plink2/formats#mendel>`_:

        - ``mydata.mendel`` -- all mendel errors: FID KID CHR SNP CODE ERROR
        - ``mydata.fmendel`` -- error count per nuclear family: FID PAT MAT CHLD N NSNP
        - ``mydata.imendel`` -- error count per individual: FID IID N NSNP
        - ``mydata.lmendel`` -- error count per variant: CHR SNP N

        **FID**, **KID**, **PAT**, **MAT**, and **IID** refer to family, kid,
        dad, mom, and individual ID, respectively, with missing values set to
        ``0``.

        SNP denotes the variant identifier ``chr:pos:ref:alt``.

        N counts all errors, while NSNP only counts SNP errors (NSNP is not in Plink).

        CHLD is the number of children in a nuclear family.

        The CODE of each Mendel error is determined by the table below,
        extending the `Plink
        classification <https://www.cog-genomics.org/plink2/basic_stats#mendel>`_.

        Those individuals implicated by each code are in bold.

        The copy state of a locus with respect to a trio is defined as follows,
        where PAR is the `pseudoautosomal region <https://en.wikipedia.org/wiki/Pseudoautosomal_region>`_ (PAR).

        - HemiX -- in non-PAR of X, male child
        - HemiY -- in non-PAR of Y, male child
        - Auto -- otherwise (in autosome or PAR, or female child)

        Any refers to :math:`\{ HomRef, Het, HomVar, NoCall \}` and ! denotes complement in this set.

        +--------+------------+------------+----------+------------------+
        |Code    | Dad        | Mom        |     Kid  |   Copy State     |
        +========+============+============+==========+==================+
        |    1   | HomVar     | HomVar     | Het      | Auto             |
        +--------+------------+------------+----------+------------------+
        |    2   | HomRef     | HomRef     | Het      | Auto             |
        +--------+------------+------------+----------+------------------+
        |    3   | HomRef     |  ! HomRef  |  HomVar  | Auto             |
        +--------+------------+------------+----------+------------------+
        |    4   |  ! HomRef  | HomRef     |  HomVar  | Auto             |
        +--------+------------+------------+----------+------------------+
        |    5   | HomRef     | HomRef     |  HomVar  | Auto             |
        +--------+------------+------------+----------+------------------+
        |    6   | HomVar     |  ! HomVar  |  HomRef  | Auto             |
        +--------+------------+------------+----------+------------------+
        |    7   |  ! HomVar  | HomVar     |  HomRef  | Auto             |
        +--------+------------+------------+----------+------------------+
        |    8   | HomVar     | HomVar     |  HomRef  | Auto             |
        +--------+------------+------------+----------+------------------+
        |    9   | Any        | HomVar     |  HomRef  | HemiX            |
        +--------+------------+------------+----------+------------------+
        |   10   | Any        | HomRef     |  HomVar  | HemiX            |
        +--------+------------+------------+----------+------------------+
        |   11   | HomVar     | Any        |  HomRef  | HemiY            |
        +--------+------------+------------+----------+------------------+
        |   12   | HomRef     | Any        |  HomVar  | HemiY            |
        +--------+------------+------------+----------+------------------+

        This method only considers children with two parents and a defined sex.

        PAR is currently defined with respect to reference
        `GRCh37 <http://www.ncbi.nlm.nih.gov/projects/genome/assembly/grc/human/>`_:

        - X: 60001 - 2699520, 154931044 - 155260560
        - Y: 10001 - 2649520, 59034050 - 59363566

        This method assumes all contigs apart from X and Y are fully autosomal;
        mitochondria, decoys, etc. are not given special treatment.

        :param str output: Output root filename.

        :param str fam: Path to .fam file.
        """

        self._jvdf.mendelErrors(output, fam)

    @handle_py4j
    def min_rep(self, max_shift=100):
        """
        Gives minimal, left-aligned representation of alleles. Note that this can change the variant position.

        **Examples**

        1. Simple trimming of a multi-allelic site, no change in variant position
        `1:10000:TAA:TAA,AA` => `1:10000:TA:T,A`

        2. Trimming of a bi-allelic site leading to a change in position
        `1:10000:AATAA,AAGAA` => `1:10002:T:G`

        :param int max_shift: maximum number of base pairs by which
          a split variant can move.  Affects memory usage, and will
          cause Hail to throw an error if a variant that moves further
          is encountered.

        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvds.minRep(max_shift)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def pca(self, scores, loadings=None, eigenvalues=None, k=10, as_array=False):
        """Run Principal Component Analysis (PCA) on the matrix of genotypes.

        **Examples**

        Compute the top 10 principal component scores, stored as sample annotations ``sa.scores.PC1``, ..., ``sa.scores.PC10`` of type Double:

        >>> vds_result = vds.pca('sa.scores')

        Compute the top 5 principal component scores, loadings, and eigenvalues, stored as annotations ``sa.scores``, ``va.loadings``, and ``global.evals`` of type Array[Double]:

        >>> vds_result = vds.pca('sa.scores', 'va.loadings', 'global.evals', 5, as_array=True)

        **Notes**

        Hail supports principal component analysis (PCA) of genotype data, a now-standard procedure `Patterson, Price and Reich, 2006 <http://journals.plos.org/plosgenetics/article?id=10.1371/journal.pgen.0020190>`_. This method expects a variant dataset with biallelic autosomal variants. Scores are computed and stored as sample annotations of type Struct by default; variant loadings and eigenvalues can optionally be computed and stored in variant and global annotations, respectively.

        PCA is based on the singular value decomposition (SVD) of a standardized genotype matrix :math:`M`, computed as follows. An :math:`n \\times m` matrix :math:`C` records raw genotypes, with rows indexed by :math:`n` samples and columns indexed by :math:`m` bialellic autosomal variants; :math:`C_{ij}` is the number of alternate alleles of variant :math:`j` carried by sample :math:`i`, which can be 0, 1, 2, or missing. For each variant :math:`j`, the sample alternate allele frequency :math:`p_j` is computed as half the mean of the non-missing entries of column :math:`j`. Entries of :math:`M` are then mean-centered and variance-normalized as

        .. math::

          M_{ij} = \\frac{C_{ij}-2p_j}{\sqrt{2p_j(1-p_j)m}},

        with :math:`M_{ij} = 0` for :math:`C_{ij}` missing (i.e. mean genotype imputation). This scaling normalizes genotype variances to a common value :math:`1/m` for variants in Hardy-Weinberg equilibrium and is further motivated in the paper cited above. (The resulting amplification of signal from the low end of the allele frequency spectrum will also introduce noise for rare variants; common practice is to filter out variants with minor allele frequency below some cutoff.)  The factor :math:`1/m` gives each sample row approximately unit total variance (assuming linkage equilibrium) and yields the sample correlation or genetic relationship matrix (GRM) as simply :math:`MM^T`.

        PCA then computes the SVD

        .. math::

          M = USV^T

        where columns of :math:`U` are left singular vectors (orthonormal in :math:`\mathbb{R}^n`), columns of :math:`V` are right singular vectors (orthonormal in :math:`\mathbb{R}^m`), and :math:`S=\mathrm{diag}(s_1, s_2, \ldots)` with ordered singular values :math:`s_1 \ge s_2 \ge \cdots \ge 0`. Typically one computes only the first :math:`k` singular vectors and values, yielding the best rank :math:`k` approximation :math:`U_k S_k V_k^T` of :math:`M`; the truncations :math:`U_k`, :math:`S_k` and :math:`V_k` are :math:`n \\times k`, :math:`k \\times k` and :math:`m \\times k` respectively.

        From the perspective of the samples or rows of :math:`M` as data, :math:`V_k` contains the variant loadings for the first :math:`k` PCs while :math:`MV_k = U_k S_k` contains the first :math:`k` PC scores of each sample. The loadings represent a new basis of features while the scores represent the projected data on those features. The eigenvalues of the GRM :math:`MM^T` are the squares of the singular values :math:`s_1^2, s_2^2, \ldots`, which represent the variances carried by the respective PCs. By default, Hail only computes the loadings if the ``loadings`` parameter is specified.

        *Note:* In PLINK/GCTA the GRM is taken as the starting point and it is computed slightly differently with regard to missing data. Here the :math:`ij` entry of :math:`MM^T` is simply the dot product of rows :math:`i` and :math:`j` of :math:`M`; in terms of :math:`C` it is

        .. math::

          \\frac{1}{m}\sum_{l\in\mathcal{C}_i\cap\mathcal{C}_j}\\frac{(C_{il}-2p_l)(C_{jl} - 2p_l)}{2p_l(1-p_l)}

        where :math:`\mathcal{C}_i = \{l \mid C_{il} \\text{ is non-missing}\}`. In PLINK/GCTA the denominator :math:`m` is replaced with the number of terms in the sum :math:`\\lvert\mathcal{C}_i\cap\\mathcal{C}_j\\rvert`, i.e. the number of variants where both samples have non-missing genotypes. While this is arguably a better estimator of the true GRM (trading shrinkage for noise), it has the drawback that one loses the clean interpretation of the loadings and scores as features and projections.

        Separately, for the PCs PLINK/GCTA output the eigenvectors of the GRM; even ignoring the above discrepancy that means the left singular vectors :math:`U_k` instead of the component scores :math:`U_k S_k`. While this is just a matter of the scale on each PC, the scores have the advantage of representing true projections of the data onto features with the variance of a score reflecting the variance explained by the corresponding feature. (In PC bi-plots this amounts to a change in aspect ratio; for use of PCs as covariates in regression it is immaterial.)

        **Annotations**

        Given root ``scores='sa.scores'`` and ``as_array=False``, :py:meth:`~hail.VariantDataset.pca` adds a Struct to sample annotations:

         - **sa.scores** (*Struct*) -- Struct of sample scores

        With ``k=3``, the Struct has three field:

         - **sa.scores.PC1** (*Double*) -- Score from first PC

         - **sa.scores.PC2** (*Double*) -- Score from second PC

         - **sa.scores.PC3** (*Double*) -- Score from third PC

        Analogous variant and global annotations of type Struct are added by specifying the ``loadings`` and ``eigenvalues`` arguments, respectively.

        Given roots ``scores='sa.scores'``, ``loadings='va.loadings'``, and ``eigenvalues='global.evals'``, and ``as_array=True``, :py:meth:`~hail.VariantDataset.pca` adds the following annotations:

         - **sa.scores** (*Array[Double]*) -- Array of sample scores from the top k PCs

         - **va.loadings** (*Array[Double]*) -- Array of variant loadings in the top k PCs

         - **global.evals** (*Array[Double]*) -- Array of the top k eigenvalues

        :param str scores: Sample annotation path to store scores.

        :param loadings: Variant annotation path to store site loadings.
        :type loadings: str or None

        :param eigenvalues: Global annotation path to store eigenvalues.
        :type eigenvalues: str or None

        :param k: Number of principal components.
        :type k: int or None

        :param bool as_array: Store annotations as type Array rather than Struct
        :type k: bool or None

        :return: Dataset with new PCA annotations.
        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvdf.pca(scores, k, joption(loadings), joption(eigenvalues), as_array)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def persist(self, storage_level="MEMORY_AND_DISK"):
        """Persist this variant dataset to memory and/or disk.

        **Examples**

        Persist the variant dataset to both memory and disk:

        >>> vds_result = vds.persist()

        **Notes**

        The :py:meth:`~hail.VariantDataset.persist` and :py:meth:`~hail.VariantDataset.cache` commands allow you to store the current dataset on disk
        or in memory to avoid redundant computation and improve the performance of Hail pipelines.

        :py:meth:`~hail.VariantDataset.cache` is an alias for :func:`persist("MEMORY_ONLY") <hail.VariantDataset.persist>`.  Most users will want "MEMORY_AND_DISK".
        See the `Spark documentation <http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence>`_ for a more in-depth discussion of persisting data.

        :param storage_level: Storage level.  One of: NONE, DISK_ONLY,
            DISK_ONLY_2, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_ONLY_SER,
            MEMORY_ONLY_SER_2, MEMORY_AND_DISK, MEMORY_AND_DISK_2,
            MEMORY_AND_DISK_SER, MEMORY_AND_DISK_SER_2, OFF_HEAP
        """

        self._jvdf.persist(storage_level)
        return self

    @property
    def global_schema(self):
        """
        Returns the signature of the global annotations contained in this VDS.

        >>> print(vds.global_schema)

        :rtype: :class:`.Type`
        """

        if self._global_schema is None:
            self._global_schema = Type._from_java(self._jvds.globalSignature())
        return self._global_schema

    @property
    def sample_schema(self):
        """
        Returns the signature of the sample annotations contained in this VDS.

        >>> print(vds.sample_schema)

        :rtype: :class:`.Type`
        """

        if self._sa_schema is None:
            self._sa_schema = Type._from_java(self._jvds.saSignature())
        return self._sa_schema

    @property
    def variant_schema(self):
        """
        Returns the signature of the variant annotations contained in this VDS.

        >>> print(vds.variant_schema)

        :rtype: :class:`.Type`
        """

        if self._va_schema is None:
            self._va_schema = Type._from_java(self._jvds.vaSignature())
        return self._va_schema

    @property
    def genotype_schema(self):
        """
        Returns the signature of the genotypes contained in this VDS.

        >>> print(vds.genotype_schema)

        :rtype: :class:`.Type`
        """

        if self._genotype_schema is None:
            self._genotype_schema = Type._from_java(self._jvds.genotypeSignature())
        return self._genotype_schema

    @handle_py4j
    def query_samples_typed(self, exprs):
        """Performs aggregation queries over samples and sample annotations, and returns Python object(s) and type(s).

        **Examples**

        >>> low_callrate_samples, t = vds.query_samples_typed(
        ...    'samples.filter(s => sa.qc.callRate < 0.95).collect()')

        See :py:meth:`.query_samples` for more information.

        :param exprs: query expressions
        :type exprs: str or list of str

        :rtype: (annotation or list of annotation,  :class:`.Type` or list of :class:`.Type`)
        """

        if isinstance(exprs, list):
            result_list = self._jvds.querySamples(jarray(Env.jvm().java.lang.String, exprs))
            ptypes = [Type._from_java(x._2()) for x in result_list]
            annotations = [ptypes[i]._convert_to_py(result_list[i]._1()) for i in xrange(len(ptypes))]
            return annotations, ptypes
        else:
            result = self._jvds.querySamples(exprs)
            t = Type._from_java(result._2())
            return t._convert_to_py(result._1()), t

    @handle_py4j
    def query_samples(self, exprs):
        """Performs aggregation queries over samples and sample annotations, and returns Python object(s).

        **Examples**

        >>> low_callrate_samples = vds.query_samples('samples.filter(s => sa.qc.callRate < 0.95).collect()')

        **Notes**

        This method evaluates Hail expressions over samples and sample
        annotations.  The ``exprs`` argument requires either a list of
        strings or a single string (which will be interpreted as a list
        with one element).  The method returns a list of results (which
        contains one element if the input parameter was a single str).

        The namespace of the expressions includes:

        - ``global``: global annotations
        - ``samples`` (*Aggregable[Sample]*): aggregable of :ref:`sample`

        Map and filter expressions on this aggregable have the additional
        namespace:

        - ``global``: global annotations
        - ``s``: sample
        - ``sa``: sample annotations

        :param exprs: query expressions
        :type exprs: str or list of str

        :rtype: annotation or list of annotation
        """

        r, t = self.query_samples_typed(exprs)
        return r

    @handle_py4j
    def query_variants_typed(self, exprs):
        """Performs aggregation queries over variants and variant annotations, and returns Python object(s) and type(s).

        **Examples**

        >>> lof_variant_count, t = vds.query_variants_typed(
        ...     'variants.filter(v => va.consequence == "LOF").count()')

        >>> [lof_variant_count, missense_count], [t1, t2] = vds.query_variants_typed([
        ...     'variants.filter(v => va.consequence == "LOF").count()',
        ...     'variants.filter(v => va.consequence == "Missense").count()'])

        See :py:meth:`.query_variants` for more information.

        :param exprs: query expressions
        :type exprs: str or list of str

        :rtype: (annotation or list of annotation, :class:`.Type` or list of :class:`.Type`)
        """
        if isinstance(exprs, list):
            result_list = self._jvds.queryVariants(jarray(Env.jvm().java.lang.String, exprs))
            ptypes = [Type._from_java(x._2()) for x in result_list]
            annotations = [ptypes[i]._convert_to_py(result_list[i]._1()) for i in xrange(len(ptypes))]
            return annotations, ptypes

        else:
            result = self._jvds.queryVariants(exprs)
            t = Type._from_java(result._2())
            return t._convert_to_py(result._1()), t

    @handle_py4j
    def query_variants(self, exprs):
        """Performs aggregation queries over variants and variant annotations, and returns Python object(s).

        **Examples**

        >>> lof_variant_count = vds.query_variants('variants.filter(v => va.consequence == "LOF").count()')

        >>> [lof_variant_count, missense_count] = vds.query_variants([
        ...     'variants.filter(v => va.consequence == "LOF").count()',
        ...     'variants.filter(v => va.consequence == "Missense").count()'])

        **Notes**

        This method evaluates Hail expressions over variants and variant
        annotations.  The ``exprs`` argument requires either a list of
        strings or a single string (which will be interpreted as a list
        with one element).  The method returns a list of results (which
        contains one element if the input parameter was a single str).

        The namespace of the expressions includes:

        - ``global``: global annotations
        - ``variants`` (*Aggregable[Variant]*): aggregable of :ref:`variant`

        Map and filter expressions on this aggregable have the additional
        namespace:

        - ``global``: global annotations
        - ``v``: :ref:`variant`
        - ``va``: variant annotations

        **Performance Note**
        It is far faster to execute multiple queries in one method than
        to execute multiple query methods.  This:

        >>> result1 = vds.query_variants('variants.count()')
        >>> result2 = vds.query_variants('variants.filter(v => v.altAllele.isSNP()).count()')

        will be nearly twice as slow as this:

        >>> exprs = ['variants.count()', 'variants.filter(v => v.altAllele.isSNP()).count()']
        >>> [num_variants, num_snps] = vds.query_variants(exprs)

        :param exprs: query expressions
        :type exprs: str or list of str

        :rtype: annotation or list of annotation
        """

        r, t = self.query_variants_typed(exprs)
        return r

    def query_genotypes_typed(self, exprs):
        """Performs aggregation queries over genotypes, and returns Python object(s) and type(s).

        **Examples**

        >>> gq_hist, t = vds.query_genotypes_typed('gs.map(g => g.gq).hist(0, 100, 100)')

        >>> [gq_hist, dp_hist], [t1, t2] = vds.query_genotypes_typed(['gs.map(g => g.gq).hist(0, 100, 100)',
        ...                                                           'gs.map(g => g.dp).hist(0, 60, 60)'])

        See :py:meth:`.query_genotypes` for more information.

        This method evaluates Hail expressions over genotypes, along with
        all variant and sample metadata for that genotype. The ``exprs``
        argument requires either a list of strings or a single string
        The method returns a list of results and a list of types (which
        each contain one element if the input parameter was a single str).

        The namespace of the expressions includes:

        - ``global``: global annotations
        - ``gs`` (*Aggregable[Genotype]*): aggregable of :ref:`genotype`

        Map and filter expressions on this aggregable have the following
        namespace:

        - ``global``: global annotations
        - ``g``: :ref:`genotype`
        - ``v``: :ref:`variant`
        - ``va``: variant annotations
        - ``s``: sample
        - ``sa``: sample annotations

        **Performance Note**
        It is far faster to execute multiple queries in one method than
        to execute multiple query methods.  This:

        >>> result1 = vds.query_genotypes('gs.count()')
        >>> result2 = vds.query_genotypes('gs.filter(g => v.altAllele.isSNP() && g.isHet).count()')

        will be nearly twice as slow as this:

        >>> exprs = ['gs.count()', 'gs.filter(g => v.altAllele.isSNP() && g.isHet).count()']
        >>> [geno_count, snp_hets] = vds.query_genotypes(exprs)

        :param exprs: query expressions
        :type exprs: str or list of str

        :rtype: (annotation or list of annotation, :class:`.Type` or list of :class:`.Type`)
        """

        if isinstance(exprs, list):
            result_list = self._jvds.queryGenotypes(jarray(Env.jvm().java.lang.String, exprs))
            ptypes = [Type._from_java(x._2()) for x in result_list]
            annotations = [ptypes[i]._convert_to_py(result_list[i]._1()) for i in xrange(len(ptypes))]
            return annotations, ptypes
        else:
            result = self._jvds.queryGenotypes(exprs)
            t = Type._from_java(result._2())
            return t._convert_to_py(result._1()), t

    def query_genotypes(self, exprs):
        """Performs aggregation queries over genotypes, and returns Python object(s).

        **Examples**

        Compute global GQ histogram

        >>> gq_hist = vds.query_genotypes('gs.map(g => g.gq).hist(0, 100, 100)')

        Compute call rate

        >>> call_rate = vds.query_genotypes('gs.fraction(g => g.isCalled)')

        Compute GQ and DP histograms

        >>> [gq_hist, dp_hist] = vds.query_genotypes(['gs.map(g => g.gq).hist(0, 100, 100)',
        ...                                                     'gs.map(g => g.dp).hist(0, 60, 60)'])


        :param exprs: query expressions
        :type exprs: str or list of str

        :rtype: annotation or list of annotation
        """

        r, t = self.query_genotypes_typed(exprs)
        return r

    @handle_py4j
    def rename_samples(self, mapping):
        """Rename samples.

        **Examples**

        >>> vds_result = vds.rename_samples({'ID1': 'id1', 'ID2': 'id2'})

        :param dict mapping: Mapping from old to new sample IDs.

        :return: Dataset with remapped sample IDs.
        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvds.renameSamples(mapping)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def repartition(self, num_partitions, shuffle=True):
        """Increase or decrease the number of variant dataset partitions.

        **Examples**

        Repartition the variant dataset to have 500 partitions:

        >>> vds_result = vds.repartition(500)

        **Notes**

        Check the current number of partitions with :py:meth:`.num_partitions`.

        The data in a variant dataset is divided into chunks called partitions, which may be stored together or across a network, so that each partition may be read and processed in parallel by available cores. When a variant dataset with :math:`M` variants is first imported, each of the :math:`k` partition will contain about :math:`M/k` of the variants. Since each partition has some computational overhead, decreasing the number of partitions can improve performance after significant filtering. Since it's recommended to have at least 2 - 4 partitions per core, increasing the number of partitions can allow one to take advantage of more cores.

        Partitions are a core concept of distributed computation in Spark, see `here <http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds>`_ for details. With ``shuffle=True``, Hail does a full shuffle of the data and creates equal sized partitions. With ``shuffle=False``, Hail combines existing partitions to avoid a full shuffle. These algorithms correspond to the ``repartition`` and ``coalesce`` commands in Spark, respectively. In particular, when ``shuffle=False``, ``num_partitions`` cannot exceed current number of partitions.

        :param int num_partitions: Desired number of partitions, must be less than the current number if ``shuffle=False``

        :param bool shuffle: If True, use full shuffle to repartition.

        :return: Variant dataset with the number of partitions equal to at most ``num_partitions``
        :rtype: :class:`.VariantDataset`
        """

        jvds = self._jvdf.coalesce(num_partitions, shuffle)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def same(self, other, tolerance=1e-6):
        """True if the two variant datasets have the same variants, samples, genotypes, and annotation schemata and values.

        **Examples**

        This will return True:

        >>> vds.same(vds)

        **Notes**

        The ``tolerance`` parameter sets the tolerance for equality when comparing floating-point fields. More precisely, :math:`x` and :math:`y` are equal if

        .. math::

            \abs{x - y} \leq tolerance * \max{\abs{x}, \abs{y}}

        :param other: variant dataset to compare against
        :type other: :class:`.VariantDataset`

        :param float tolerance: floating-point tolerance for equality

        :rtype: bool
        """

        return self._jvds.same(other._jvds, tolerance)

    @handle_py4j
    def sample_qc(self):
        """Compute per-sample QC metrics.

        **Annotations**

        :py:meth:`~hail.VariantDataset.sample_qc` computes 20 sample statistics from the genotype data and stores the results as sample annotations that can be accessed with ``sa.qc.<identifier>``:

        +---------------------------+--------+---------------------------------------------------------+
        | Name                      | Type   | Description                                             |
        +===========================+========+=========================================================+
        | ``callRate``              | Double | Fraction of variants with called genotypes              |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nHomRef``               | Int    | Number of homozygous reference variants                 |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nHet``                  | Int    | Number of heterozygous variants                         |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nHomVar``               | Int    | Number of homozygous alternate variants                 |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nCalled``               | Int    | Sum of ``nHomRef`` + ``nHet`` + ``nHomVar``             |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nNotCalled``            | Int    | Number of uncalled variants                             |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nSNP``                  | Int    | Number of SNP variants                                  |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nInsertion``            | Int    | Number of insertion variants                            |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nDeletion``             | Int    | Number of deletion variants                             |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nSingleton``            | Int    | Number of private variants                              |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nTransition``           | Int    | Number of transition (A-G, C-T) variants                |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nTransversion``         | Int    | Number of transversion variants                         |
        +---------------------------+--------+---------------------------------------------------------+
        | ``nNonRef``               | Int    | Sum of ``nHet`` and ``nHomVar``                         |
        +---------------------------+--------+---------------------------------------------------------+
        | ``rTiTv``                 | Double | Transition/Transversion ratio                           |
        +---------------------------+--------+---------------------------------------------------------+
        | ``rHetHomVar``            | Double | Het/HomVar ratio across all variants                    |
        +---------------------------+--------+---------------------------------------------------------+
        | ``rInsertionDeletion``    | Double | Insertion/Deletion ratio across all variants            |
        +---------------------------+--------+---------------------------------------------------------+
        | ``dpMean``                | Double | Depth mean across all variants                          |
        +---------------------------+--------+---------------------------------------------------------+
        | ``dpStDev``               | Double | Depth standard deviation across all variants            |
        +---------------------------+--------+---------------------------------------------------------+
        | ``gqMean``                | Double | The average genotype quality across all variants        |
        +---------------------------+--------+---------------------------------------------------------+
        | ``gqStDev``               | Double | Genotype quality standard deviation across all variants |
        +---------------------------+--------+---------------------------------------------------------+

        Missing values ``NA`` may result (for example, due to division by zero) and are handled properly in filtering and written as "NA" in export modules. The empirical standard deviation is computed with zero degrees of freedom.

        :return: Annotated variant dataset with new sample qc annotations.
        :rtype: :class:`.VariantDataset`
        """

        return VariantDataset(self.hc, self._jvdf.sampleQC())

    @handle_py4j
    def storage_level(self):
        """Returns the storage (persistence) level of the variant dataset.

        **Notes**

        See the `Spark documentation <http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence>`_ for details on persistence levels.

        :rtype: str
        """

        return self._jvds.storageLevel()

    @handle_py4j
    def split_multi(self, propagate_gq=False, keep_star_alleles=False, max_shift=100):
        """Split multiallelic variants.

        **Examples**

        >>> vds.split_multi().write('output/split.vds')

        **Notes**

        We will explain by example. Consider a hypothetical 3-allelic
        variant:

        .. code-block:: text

          A   C,T 0/2:7,2,6:15:45:99,50,99,0,45,99

        split_multi will create two biallelic variants (one for each
        alternate allele) at the same position

        .. code-block:: text

          A   C   0/0:13,2:15:45:0,45,99
          A   T   0/1:9,6:15:50:50,0,99

        Each multiallelic GT field is downcoded once for each
        alternate allele. A call for an alternate allele maps to 1 in
        the biallelic variant corresponding to itself and 0
        otherwise. For example, in the example above, 0/2 maps to 0/0
        and 0/1. The genotype 1/2 maps to 0/1 and 0/1.

        The biallelic alt AD entry is just the multiallelic AD entry
        corresponding to the alternate allele. The ref AD entry is the
        sum of the other multiallelic entries.

        The biallelic DP is the same as the multiallelic DP.

        The biallelic PL entry for for a genotype g is the minimum
        over PL entries for multiallelic genotypes that downcode to
        g. For example, the PL for (A, T) at 0/1 is the minimum of the
        PLs for 0/1 (50) and 1/2 (45), and thus 45.

        Fixing an alternate allele and biallelic variant, downcoding
        gives a map from multiallelic to biallelic alleles and
        genotypes. The biallelic AD entry for an allele is just the
        sum of the multiallelic AD entries for alleles that map to
        that allele. Similarly, the biallelic PL entry for a genotype
        is the minimum over multiallelic PL entries for genotypes that
        map to that genotype.

        By default, GQ is recomputed from PL. If ``propagate_gq=True``
        is passed, the biallelic GQ field is simply the multiallelic
        GQ field, that is, genotype qualities are unchanged.

        Here is a second example for a het non-ref

        .. code-block:: text

          A   C,T 1/2:2,8,6:16:45:99,50,99,45,0,99

        splits as

        .. code-block:: text

          A   C   0/1:8,8:16:45:45,0,99
          A   T   0/1:10,6:16:50:50,0,99

        **VCF Info Fields**

        Hail does not split annotations in the info field. This means
        that if a multiallelic site with ``info.AC`` value ``[10, 2]`` is
        split, each split site will contain the same array ``[10,
        2]``. The provided allele index annotation ``va.aIndex`` can be used
        to select the value corresponding to the split allele's
        position:

        >>> vds_result = (vds.split_multi()
        ...     .filter_variants_expr('va.info.AC[va.aIndex - 1] < 10', keep = False))

        VCFs split by Hail and exported to new VCFs may be
        incompatible with other tools, if action is not taken
        first. Since the "Number" of the arrays in split multiallelic
        sites no longer matches the structure on import ("A" for 1 per
        allele, for example), Hail will export these fields with
        number ".".

        If the desired output is one value per site, then it is
        possible to use annotate_variants_expr to remap these
        values. Here is an example:

        >>> (vds.split_multi()
        ...     .annotate_variants_expr('va.info.AC = va.info.AC[va.aIndex - 1]')
        ...     .export_vcf('output/export.vcf'))

        The info field AC in *data/export.vcf* will have ``Number=1``.

        **Annotations**

        :py:meth:`~hail.VariantDataset.split_multi` adds the
        following annotations:

         - **va.wasSplit** (*Boolean*) -- true if this variant was
           originally multiallelic, otherwise false.
         - **va.aIndex** (*Int*) -- The original index of this
           alternate allele in the multiallelic representation (NB: 1
           is the first alternate allele or the only alternate allele
           in a biallelic variant). For example, 1:100:A:T,C splits
           into two variants: 1:100:A:T with ``aIndex = 1`` and
           1:100:A:C with ``aIndex = 2``.

        :param bool propagate_gq: Set the GQ of output (split)
          genotypes to be the GQ of the input (multi-allelic) variants
          instead of recompute GQ as the difference between the two
          smallest PL values.  Intended to be used in conjunction with
          ``import_vcf(store_gq=True)``.  This option will be obviated
          in the future by generic genotype schemas.  Experimental.
        :param bool keep_star_alleles: Do not filter out * alleles.
        :param int max_shift: maximum number of base pairs by which
          a split variant can move.  Affects memory usage, and will
          cause Hail to throw an error if a variant that moves further
          is encountered.

        :return: A biallelic variant dataset.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvdf.splitMulti(propagate_gq, keep_star_alleles, max_shift)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def tdt(self, fam, root='va.tdt'):
        """Find transmitted and untransmitted variants; count per variant and
        nuclear family.

        **Examples**

        Compute TDT association results:

        >>> (vds.tdt("data/trios.fam")
        ...     .export_variants("output/tdt_results.tsv", "Variant = v, va.tdt.*"))

        **Notes**

        The transmission disequilibrium test tracks the number of times the alternate allele is transmitted (t) or not transmitted (u) from a heterozgyous parent to an affected child under the null that the rate of such transmissions is 0.5.  For variants where transmission is guaranteed (i.e., the Y chromosome, mitochondria, and paternal chromosome X variants outside of the PAR), the test cannot be used.

        The TDT statistic is given by

        .. math::

            (t-u)^2 \over (t+u)

        and follows a 1 degree of freedom chi-squared distribution under the null hypothesis.


        The number of transmissions and untransmissions for each possible set of genotypes is determined from the table below.  The copy state of a locus with respect to a trio is defined as follows, where PAR is the pseudoautosomal region (PAR).

        - HemiX -- in non-PAR of X and child is male
        - Auto -- otherwise (in autosome or PAR, or child is female)

        +--------+--------+--------+------------+---+---+
        |  Kid   | Dad    | Mom    | Copy State | T | U |
        +========+========+========+============+===+===+
        | HomRef | Het    | Het    | Auto       | 0 | 2 |
        +--------+--------+--------+------------+---+---+
        | HomRef | HomRef | Het    | Auto       | 0 | 1 |
        +--------+--------+--------+------------+---+---+
        | HomRef | Het    | HomRef | Auto       | 0 | 1 |
        +--------+--------+--------+------------+---+---+
        | Het    | Het    | Het    | Auto       | 1 | 1 |
        +--------+--------+--------+------------+---+---+
        | Het    | HomRef | Het    | Auto       | 1 | 0 |
        +--------+--------+--------+------------+---+---+
        | Het    | Het    | HomRef | Auto       | 1 | 0 |
        +--------+--------+--------+------------+---+---+
        | Het    | HomVar | Het    | Auto       | 0 | 1 |
        +--------+--------+--------+------------+---+---+
        | Het    | Het    | HomVar | Auto       | 0 | 1 |
        +--------+--------+--------+------------+---+---+
        | HomVar | Het    | Het    | Auto       | 2 | 0 |
        +--------+--------+--------+------------+---+---+
        | HomVar | Het    | HomVar | Auto       | 1 | 0 |
        +--------+--------+--------+------------+---+---+
        | HomVar | HomVar | Het    | Auto       | 1 | 0 |
        +--------+--------+--------+------------+---+---+
        | HomRef | HomRef | Het    | HemiX      | 0 | 1 |
        +--------+--------+--------+------------+---+---+
        | HomRef | HomVar | Het    | HemiX      | 0 | 1 |
        +--------+--------+--------+------------+---+---+
        | HomVar | HomRef | Het    | HemiX      | 1 | 0 |
        +--------+--------+--------+------------+---+---+
        | HomVar | HomVar | Het    | HemiX      | 1 | 0 |
        +--------+--------+--------+------------+---+---+


        :py:meth:`~hail.VariantDataset.tdt` only considers complete trios (two parents and a proband) with defined sex.

        PAR is currently defined with respect to reference `GRCh37 <http://www.ncbi.nlm.nih.gov/projects/genome/assembly/grc/human/>`_:

        - X: 60001-2699520
        - X: 154931044-155260560
        - Y: 10001-2649520
        - Y: 59034050-59363566

        :py:meth:`~hail.VariantDataset.tdt` assumes all contigs apart from X and Y are fully autosomal; decoys, etc. are not given special treatment.

        **Annotations**

        :py:meth:`~hail.VariantDataset.tdt` adds the following annotations:

         - **tdt.nTransmitted** (*Int*) -- Number of transmitted alternate alleles.

         - **va.tdt.nUntransmitted** (*Int*) -- Number of untransmitted alternate alleles.

         - **va.tdt.chi2** (*Double*) -- TDT statistic.

         - **va.tdt.pval** (*Double*) -- p-value.

        :param str fam: Path to FAM file.

        :param root: Variant annotation root to store TDT result.

        :return: Variant dataset with TDT association results added to variant annotations.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvdf.tdt(fam, root)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def _typecheck(self):
        """Check if all sample, variant and global annotations are consistent with the schema."""

        self._jvds.typecheck()

    @handle_py4j
    def variant_qc(self):
        """Compute common variant statistics (quality control metrics).

        **Examples**

        >>> vds_result = vds.variant_qc()

        .. _variantqc_annotations:

        **Annotations**

        :py:meth:`~hail.VariantDataset.variant_qc` computes 18 variant statistics from the genotype data and stores the results as variant annotations that can be accessed with ``va.qc.<identifier>``:

        +---------------------------+--------+--------------------------------------------------------+
        | Name                      | Type   | Description                                            |
        +===========================+========+========================================================+
        | ``callRate``              | Double | Fraction of samples with called genotypes              |
        +---------------------------+--------+--------------------------------------------------------+
        | ``AF``                    | Double | Calculated minor allele frequency (q)                  |
        +---------------------------+--------+--------------------------------------------------------+
        | ``AC``                    | Int    | Count of alternate alleles                             |
        +---------------------------+--------+--------------------------------------------------------+
        | ``rHeterozygosity``       | Double | Proportion of heterozygotes                            |
        +---------------------------+--------+--------------------------------------------------------+
        | ``rHetHomVar``            | Double | Ratio of heterozygotes to homozygous alternates        |
        +---------------------------+--------+--------------------------------------------------------+
        | ``rExpectedHetFrequency`` | Double | Expected rHeterozygosity based on HWE                  |
        +---------------------------+--------+--------------------------------------------------------+
        | ``pHWE``                  | Double | p-value from Hardy Weinberg Equilibrium null model     |
        +---------------------------+--------+--------------------------------------------------------+
        | ``nHomRef``               | Int    | Number of homozygous reference samples                 |
        +---------------------------+--------+--------------------------------------------------------+
        | ``nHet``                  | Int    | Number of heterozygous samples                         |
        +---------------------------+--------+--------------------------------------------------------+
        | ``nHomVar``               | Int    | Number of homozygous alternate samples                 |
        +---------------------------+--------+--------------------------------------------------------+
        | ``nCalled``               | Int    | Sum of ``nHomRef``, ``nHet``, and ``nHomVar``          |
        +---------------------------+--------+--------------------------------------------------------+
        | ``nNotCalled``            | Int    | Number of uncalled samples                             |
        +---------------------------+--------+--------------------------------------------------------+
        | ``nNonRef``               | Int    | Sum of ``nHet`` and ``nHomVar``                        |
        +---------------------------+--------+--------------------------------------------------------+
        | ``rHetHomVar``            | Double | Het/HomVar ratio across all samples                    |
        +---------------------------+--------+--------------------------------------------------------+
        | ``dpMean``                | Double | Depth mean across all samples                          |
        +---------------------------+--------+--------------------------------------------------------+
        | ``dpStDev``               | Double | Depth standard deviation across all samples            |
        +---------------------------+--------+--------------------------------------------------------+
        | ``gqMean``                | Double | The average genotype quality across all samples        |
        +---------------------------+--------+--------------------------------------------------------+
        | ``gqStDev``               | Double | Genotype quality standard deviation across all samples |
        +---------------------------+--------+--------------------------------------------------------+

        Missing values ``NA`` may result (for example, due to division by zero) and are handled properly in filtering and written as "NA" in export modules. The empirical standard deviation is computed with zero degrees of freedom.

        :return: Annotated variant dataset with new variant QC annotations.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvdf.variantQC()
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def vep(self, config, block_size=1000, root='va.vep', csq=False):
        """Annotate variants with VEP.

        :py:meth:`~hail.VariantDataset.vep` runs `Variant Effect Predictor <http://www.ensembl.org/info/docs/tools/vep/index.html>`_ with
        the `LOFTEE plugin <https://github.com/konradjk/loftee>`_
        on the current variant dataset and adds the result as a variant annotation.

        If the variant annotation path defined by ``root`` already exists and its schema matches the VEP schema, then
        Hail only runs VEP for variants for which the annotation is missing.

        **Examples**

        Add VEP annotations to the dataset:

        >>> vds_result = vds.vep("data/vep.properties") # doctest: +SKIP

        **Configuration**

        :py:meth:`~hail.VariantDataset.vep` needs a configuration file to tell it how to run
        VEP. The format is a `.properties file <https://en.wikipedia.org/wiki/.properties>`_.
        Roughly, each line defines a property as a key-value pair of the form `key = value`. `vep` supports the following properties:

        - **hail.vep.perl** -- Location of Perl. Optional, default: perl.
        - **hail.vep.perl5lib** -- Value for the PERL5LIB environment variable when invoking VEP. Optional, by default PERL5LIB is not set.
        - **hail.vep.path** -- Value of the PATH environment variable when invoking VEP.  Optional, by default PATH is not set.
        - **hail.vep.location** -- Location of the VEP Perl script.  Required.
        - **hail.vep.cache_dir** -- Location of the VEP cache dir, passed to VEP with the `--dir` option.  Required.
        - **hail.vep.fasta** -- Location of the FASTA file to use to look up the reference sequence, passed to VEP with the `--fasta` option.  Required.
        - **hail.vep.lof.human_ancestor** -- Location of the human ancestor file for the LOFTEE plugin.  Required.
        - **hail.vep.lof.conservation_file** -- Location of the conservation file for the LOFTEE plugin.  Required.

        Here is an example `vep.properties` configuration file

        .. code-block:: text

            hail.vep.perl = /usr/bin/perl
            hail.vep.path = /usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
            hail.vep.location = /path/to/vep/ensembl-tools-release-81/scripts/variant_effect_predictor/variant_effect_predictor.pl
            hail.vep.cache_dir = /path/to/vep
            hail.vep.lof.human_ancestor = /path/to/loftee_data/human_ancestor.fa.gz
            hail.vep.lof.conservation_file = /path/to/loftee_data//phylocsf.sql

        **VEP Invocation**

        .. code-block:: text

            <hail.vep.perl>
            <hail.vep.location>
            --format vcf
            --json
            --everything
            --allele_number
            --no_stats
            --cache --offline
            --dir <hail.vep.cache_dir>
            --fasta <hail.vep.cache_dir>/homo_sapiens/81_GRCh37/Homo_sapiens.GRCh37.75.dna.primary_assembly.fa
            --minimal
            --assembly GRCh37
            --plugin LoF,human_ancestor_fa:$<hail.vep.lof.human_ancestor>,filter_position:0.05,min_intron_size:15,conservation_file:<hail.vep.lof.conservation_file>
            -o STDOUT

        **Annotations**

        Annotations with the following schema are placed in the location specified by ``root``.
        The schema can be confirmed with :py:attr:`~hail.VariantDataset.variant_schema`, :py:attr:`~hail.VariantDataset.sample_schema`, and :py:attr:`~hail.VariantDataset.global_schema`.

        .. code-block:: text

            Struct{
              assembly_name: String,
              allele_string: String,
              colocated_variants: Array[Struct{
                aa_allele: String,
                aa_maf: Double,
                afr_allele: String,
                afr_maf: Double,
                allele_string: String,
                amr_allele: String,
                amr_maf: Double,
                clin_sig: Array[String],
                end: Int,
                eas_allele: String,
                eas_maf: Double,
                ea_allele: String,,
                ea_maf: Double,
                eur_allele: String,
                eur_maf: Double,
                exac_adj_allele: String,
                exac_adj_maf: Double,
                exac_allele: String,
                exac_afr_allele: String,
                exac_afr_maf: Double,
                exac_amr_allele: String,
                exac_amr_maf: Double,
                exac_eas_allele: String,
                exac_eas_maf: Double,
                exac_fin_allele: String,
                exac_fin_maf: Double,
                exac_maf: Double,
                exac_nfe_allele: String,
                exac_nfe_maf: Double,
                exac_oth_allele: String,
                exac_oth_maf: Double,
                exac_sas_allele: String,
                exac_sas_maf: Double,
                id: String,
                minor_allele: String,
                minor_allele_freq: Double,
                phenotype_or_disease: Int,
                pubmed: Array[Int],
                sas_allele: String,
                sas_maf: Double,
                somatic: Int,
                start: Int,
                strand: Int
              }],
              end: Int,
              id: String,
              input: String,
              intergenic_consequences: Array[Struct{
                allele_num: Int,
                consequence_terms: Array[String],
                impact: String,
                minimised: Int,
                variant_allele: String
              }],
              most_severe_consequence: String,
              motif_feature_consequences: Array[Struct{
                allele_num: Int,
                consequence_terms: Array[String],
                high_inf_pos: String,
                impact: String,
                minimised: Int,
                motif_feature_id: String,
                motif_name: String,
                motif_pos: Int,
                motif_score_change: Double,
                strand: Int,
                variant_allele: String
              }],
              regulatory_feature_consequences: Array[Struct{
                allele_num: Int,
                biotype: String,
                consequence_terms: Array[String],
                impact: String,
                minimised: Int,
                regulatory_feature_id: String,
                variant_allele: String
              }],
              seq_region_name: String,
              start: Int,
              strand: Int,
              transcript_consequences: Array[Struct{
                allele_num: Int,
                amino_acids: String,
                biotype: String,
                canonical: Int,
                ccds: String,
                cdna_start: Int,
                cdna_end: Int,
                cds_end: Int,
                cds_start: Int,
                codons: String,
                consequence_terms: Array[String],
                distance: Int,
                domains: Array[Struct{
                  db: String
                  name: String
                }],
                exon: String,
                gene_id: String,
                gene_pheno: Int,
                gene_symbol: String,
                gene_symbol_source: String,
                hgnc_id: Int,
                hgvsc: String,
                hgvsp: String,
                hgvs_offset: Int,
                impact: String,
                intron: String,
                lof: String,
                lof_flags: String,
                lof_filter: String,
                lof_info: String,
                minimised: Int,
                polyphen_prediction: String,
                polyphen_score: Double,
                protein_end: Int,
                protein_start: Int,
                protein_id: String,
                sift_prediction: String,
                sift_score: Double,
                strand: Int,
                swissprot: String,
                transcript_id: String,
                trembl: String,
                uniparc: String,
                variant_allele: String
              }],
              variant_class: String
            }

        :param str config: Path to VEP configuration file.

        :param block_size: Number of variants to annotate per VEP invocation.
        :type block_size: int

        :param str root: Variant annotation path to store VEP output.

        :param bool force: If True, force VEP annotation from scratch.

        :param bool csq: If True, annotates VCF CSQ field as a String.
            If False, annotates with the full nested struct schema

        :return: An annotated with variant annotations from VEP.
        :rtype: :py:class:`.VariantDataset`
        """

        jvds = self._jvds.vep(config, root, csq, block_size)
        return VariantDataset(self.hc, jvds)

    @handle_py4j
    def variants_keytable(self):
        """Convert variants and variant annotations to a KeyTable.

        The resulting KeyTable has schema:

        .. code-block:: text

          Struct {
            v: Variant
            va: variant annotations
          }

        with a single key ``v``.

        :return: Key table with variants and variant annotations.
        :rtype: :class:`.KeyTable`
        """

        return KeyTable(self.hc, self._jvds.variantsKT())

    @handle_py4j
    def samples_keytable(self):
        """Convert samples and sample annotations to KeyTable.

        The resulting KeyTable has schema:

        .. code-block:: text

          Struct {
            s: Sample
            sa: sample annotations
          }

        with a single key ``s``.

        :return: Key table with samples and sample annotations.
        :rtype: :class:`.KeyTable`
        """

        return KeyTable(self.hc, self._jvds.samplesKT())

    @handle_py4j
    def make_keytable(self, variant_expr, genotype_expr, key_names=[]):
        """Make a KeyTable with one row per variant.

        Per sample field names in the result are formed by concatenating the
        sample ID with the ``genotype_expr`` left hand side with dot (.).
        If the left hand side is empty::

          `` = expr

        then the dot (.) is ommited.

        **Examples**

        Consider a :py:class:`VariantDataset` ``vds`` with 2 variants and 3 samples:

        .. code-block:: text

          Variant	FORMAT	A	B	C
          1:1:A:T	GT:GQ	0/1:99	./.	0/0:99
          1:2:G:C	GT:GQ	0/1:89	0/1:99	1/1:93

        Then:

        >>> kt = vds.make_keytable('v = v', ['gt = g.gt', 'gq = g.gq'], [])

        returns a :py:class:`KeyTable` with schema

        .. code-block:: text

            v: Variant
            A.gt: Int
            B.gt: Int
            C.gt: Int
            A.gq: Int
            B.gq: Int
            C.gq: Int

        in particular, the values would be

        .. code-block:: text

            v	A.gt	B.gt	C.gt	A.gq	B.gq	C.gq
            1:1:A:T	1	NA	0	99	NA	99
            1:2:G:C	1	1	2	89	99	93

        :param variant_expr: Variant annotation expressions.
        :type variant_expr: str or list of str

        :param genotype_expr: Genotype annotation expressions.
        :type genotype_expr: str or list of str

        :param key_names: list of key columns
        :type key_names: list of str

        :rtype: :py:class:`.KeyTable`
        """

        if isinstance(variant_expr, list):
            variant_expr = ','.join(variant_expr)
        if isinstance(genotype_expr, list):
            genotype_expr = ','.join(genotype_expr)

        jkt = self._jvds.makeKT(variant_expr, genotype_expr,
                                jarray(Env.jvm().java.lang.String, key_names))
        return KeyTable(self.hc, jkt)
