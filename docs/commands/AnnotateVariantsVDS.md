# `annotatevariants vds`

This module is a subcommand of `annotatevariants`, and annotates variants from another VDS.

#### Command Line Arguments

Argument | Shortcut | Default | Description
:-:  | :-: |:-: | ---
`--input <vds-path>` | `-i` | **Required** | Path to VDS
`--root <root>` | `-r` | **Required** | Annotation path root: period-delimited path starting with `va`

____

#### Examples

____

```
$ hail read /user/me/myfile.vds showannotations
hail: info: running: read /user/me/myfile.vds
hail: info: running: showannotations
Sample annotations:
sa: Empty

Variant annotations:
va: va.<identifier>
  rsid: String
  qual: Double
  filters: Set[String]
  pass: Boolean
  info: va.info.<identifier>
    AC: Int
  custom_annotation_1: Double
```

The above VDS file was imported from a VCF, and thus contains all the expected annotations from VCF files, as well as one user-added custom annotation (`va.custom_annotation_1`).  The proper command line to import it is below:

```
$ hail [read / import / previous commands] \
    annotatevariants vds \
        -i /user/me/myfile.vds \
        -r va.other \
```

The schema produced will look like this:

```
Variant annotations:
va: Struct {
    <probably lots of other stuff here>
    other: Struct {
        rsid: String
        qual: Double
        filters: Set[String]
        pass: Boolean
        info: Struct {
            AC: Int
        }
        custom_annotation_1: Double
    }
}
```
