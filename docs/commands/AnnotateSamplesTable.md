# `annotatesamples table`

This module is a subcommand of `annotatesamples`, and loads a text file into sample annotations using a sample ID key.

#### Command Line Arguments

Argument | Shortcut | Default | Description
:-:  | :-: |:-: | ---
`--input <file>` | `-i` | **Required** | Path to file
`--root <root>` | `-r` | **Required** | Annotation path root: period-delimited path starting with `sa`
`--sampleheader <sample-col>` | `-s` | `Sample` | Column name of the sample ID key
`--types <type-code>` | `-t` | **all Strings** | specify data types of fields, in a comma-delimited string of `name: Type` elements
`--missing <missing-val>` | `-m` | `NA` | Indicate the missing value in the table, if not "NA"
`--delimiter <sep>` | `-d` | `\t` | Indicate the field delimiter

____

#### Example

We have a file with phenotypes and age:
```
$ cat ~/samples.tsv
Sample  Phenotype1   Phenotype2  Age
PT-1234 24.15        ADHD        24
PT-1235 31.01        ADHD        25
PT-1236 25.95        Control     19
PT-1237 26.80        Control     42
PT-1238 NA           ADHD        89
PT-1239 27.53        Control     55
```

To annotate from this file, one must a) specify where to put it in sample annotations, and b) specify the types of `Phenotype1` and `Age`.  The sample header agrees with the default ("Sample") and the missingness is encoded as "NA", also the default.  The Hail command line should look like:

```
$ hail [read / import / previous commands] \
    annotatesamples table \
        -i file:///user/me/samples.tsv \
        -t "Phenotype1: Double, Age: Int" \
        -r sa.phenotypes
```

   This will read the file and produce annotations of the following schema:

```
Sample annotations:
sa: sa.<identifier>
    phenotypes: sa.phenotypes.<identifier>
        Phenotype1: Double
        Phenotype2: String
        Age: Int
```

____

**Example 2:**

```
$ cat ~/samples2.tsv
Batch   PT-ID
1kg     PT-0001
1kg     PT-0002
study1  PT-0003
study3  PT-0003
.       PT-0004
1kg     PT-0005
.       PT-0006
1kg     PT-0007
```

This file does not have non-string types, but it does have a sample column identifier that is not "Sample", and missingness encoded by "." instead of "NA".  The command line should read:

```
$ hail [read / import, previous commands] \
    annotatesamples table \
        -i file:///user/me/samples2.tsv \
        --sampleheader PT-ID \
        --missing "." \
        --root sa.group1.batch
```
