# `annotatevariants table`

This module is a subcommand of `annotatevariants`, and loads a text file into variant annotations using a variant key or chr/pos/ref/alt keys.

#### Command Line Arguments

Argument | Shortcut | Default | Description
:-:  | :-: |:-: | ---
`<files...>` | `--` | **Required** | Input files (supports globbing e.g. `files.chr*.tsv`), header read from first appearing file
`--root <root>` | `-r` | **Required** | Annotation path root: period-delimited path starting with `va`
`--vcolumns <variant col/cols>` | `-v` | `Chromosome, Position, Ref, Alt` | Column name of either variant key (CHR:POS:REF:ALT format) or four keys for Chromosome, Position, Ref, Alt
`--types <type-code>` | `-t` | **all Strings** | specify data types of fields, in a comma-delimited string of `name: Type` elements
`--missing <missing-val>` | `-m` | `NA` | Indicate the missing value in the table, if not "NA"
`--delimiter <sep>` | `-d` | `\t` | Indicate the field delimiter

____

#### Examples

**Example 1**
```
$ zcat ~/consequences.tsv.gz
Variant             Consequence     DNAseSensitivity
1:2001020:A:T       Synonymous      0.86
1:2014122:TTC:T     Frameshift      0.65
1:2015242:T:G       Missense        0.77
1:2061928:C:CCCC    Intergenic      0.12
1:2091230:G:C       Synonymous      0.66
```

This file contains one field to identify the variant and two data columns: one which encodes a string and one which encodes a double.  The command line should appear as:

```
$ hail [read / import / previous commands] \
    annotatevariants table \
        file:///user/me/consequences.tsv.gz \
        -t "DNAseSensitivity: Double" \
        -r va.varianteffects \
        -v Variant
```

This invocation will annotate variants with the following schema:

```
Variant annotations:
va: Struct {
    <probably lots of other stuff here>
    varianteffects: Struct {
        Consequence: String
        DNAseSensitivity: Double
    }
}
```

____

**Example 2**

```
$ zcat ~/ExAC_Counts.tsv.gz
Chr  Pos        Ref     Alt     AC
16   29501233   A       T       1
16   29561200   TTTTT   T       15023
16   29582880   G       C       10

```

In this case, the variant is indicated by four columns, but the header does not match the default ("Chromosome, Position, Ref, Alt").  The proper command line is below:

```
$ hail [read / import / previous commands] \
    annotatevariants table \
        file:///user/me/ExAC_Counts.tsv.gz \
        -t "AC: Int" \
        -r va.exac \
        -v "Chr,Pos,Ref,Alt"
```

And the schema:

```
Variant annotations:
va: Struct {
    <probably lots of other stuff here>
    exac: Struct {
        AC: Int
    }
}
```
