Informe.pdf - El informe...
Presentacion.pdf- La presentacion...
*.png - Gráficas incluidas en el informe, en su tamaño original
jupyter/ Cuaderno python con el analisis de los datos
scripts/ Scripts con los que se generaron los datos:
	-bitcoin2igraph.py: Lee el registro bitcoin y lo pasa a un igraph
	-dataResume.py: Muestra estadisticas de los grafos
	-entityAnalizer: Muestra estadisticas de los grafos
	-equivalence.py: Librería que analiza clases de equivalencia
	-unionFind.py: Basado en la heuristica de asociacion de direcciones, encuentra clases de equivalencia y une conjuntos de direcciones en un mismo nodo Wallet.	
	-entityRecognize.py: Encuentra entre los grafos generados cuales de las Wallets son las mismas y les asigna el mismo nombre en todos lso grafos.
	-tagger.py: Clasifica los nodos en Wallets, Servicios, Gambling, Otros
	-joiner.py: Junta todos los grafos y los exporta a un unico archivo csv que puede ser importado a neo4j.
