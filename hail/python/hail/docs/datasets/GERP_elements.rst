.. _GERP_elements:

GERP_elements
=============

*  **Versions:** GERP++
*  **Reference genome builds:** GRCh37, GRCh38
*  **Type:** :class:`Table`

Schema (GERP++, GRCh37)
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: text

    ----------------------------------------
    Global fields:
        'metadata': struct {
            name: str, 
            version: str, 
            reference_genome: str, 
            n_rows: int32, 
            n_partitions: int32
        } 
    ----------------------------------------
    Row fields:
        'interval': interval<locus<GRCh37>> 
        'S': float64 
        'p_value': float64 
    ----------------------------------------
    Key: ['interval']
    ----------------------------------------
    
