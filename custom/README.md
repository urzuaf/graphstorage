 Ingesta WORM de nodes.pgdf y edges.pgdf hacia un formato binario eficiente.
 Puro Java. Solo ingesta; no implementa consultas.

  Layout de salida dentro de outDir:
 
   nodes.rec          # registros de nodos (por ordinal)
   nodes.off          # offsets de nodo (ordinal -> offset en nodes.rec)       [u64 * N]
   nodes.id.str       # pool de strings de nodeId (UTF-8)
   nodes.id.ord2pos   # ordinal -> [off,len] en nodes.id.str                   [u32,u32] * N
   nodes.id.lex       # array ordenado por string: [off,len,ordinal]           [u32,u32,u32] * N
 
   edges.rec          # registros de aristas (por ordinal)
   edges.off          # offsets de arista (ordinal -> offset en edges.rec)     [u64 * M]
   edges.id.str       # pool de strings de edgeId
   edges.id.ord2pos   # ordinal -> [off,len] en edges.id.str                   [u32,u32] * M
   edges.id.lex       #  igual que nodes.id.lex
 
   dict.labels.str    # pool de labels
   dict.labels.lex    # array ordenado: [off,len,labelId]                      [u32,u32,u32] * L
 
   dict.propname.str  # pool de nombres de propiedades
   dict.propname.lex  # array ordenado: [off,len,propNameId]                   [u32,u32,u32] * PN
 
   dict.propval.str   # pool de valores (normalizados lower-case)
   dict.propval.lex   # array ordenado: [off,len,propValId]                    [u32,u32,u32] * PV
 
   idx.edgesByLabel.dir  # [labelId:u32, off:u64, count:u32] * L_e
   idx.edgesByLabel.pl   # posting lists de edgeOrdinal (varint + delta)
 
   idx.srcByLabel.dir    # [labelId:u32, off:u64, count:u32] * L_s
   idx.srcByLabel.pl     # posting lists de nodeOrdinal (varint + delta)
 
   idx.dstByLabel.dir    # [labelId:u32, off:u64, count:u32] * L_d
   idx.dstByLabel.pl     # posting lists de nodeOrdinal (varint + delta)
 
   idx.nodesByProp.dir   # [propNameId:u32, propValId:u32, off:u64, count:u32] * K
   idx.nodesByProp.pl    # posting lists de nodeOrdinal (varint + delta)
 