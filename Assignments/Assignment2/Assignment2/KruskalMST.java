

import java.util.Scanner;

import Sp6.PrimMST.PrimVertex;

import cs6301.g00.GraphAlgorithm;
import cs6301.g00.Timer;
import cs6301.g00.Graph.Edge;
import cs6301.g00.Graph.Vertex;


import java.io.FileNotFoundException;
import java.io.File;

public class KruskalMST extends GraphAlgorithm<PrimMST.PrimVertex> {
	public KruskalMST(Graph g) {

		super(g);
		
		
		for(Vertex v:g){
			
		}
		for(Edge e:){
			
			
			
		}

		node = new PrimVertex[g.size()];
		// Create array for storing vertex properties
		for (Vertex u : g) {
			node[u.getName()] = new PrimVertex(u);
		}

	}

	public int kruskal() {
		int wmst = 0;

		return wmst;
	}

	public static void main(String[] args) throws FileNotFoundException {
		Scanner in;

		if (args.length > 0) {
			File inputFile = new File(args[0]);
			in = new Scanner(inputFile);
		} else {
			in = new Scanner(System.in);
		}

		Graph g = Graph.readGraph(in);
		Graph.Vertex s = g.getVertex(1);

		Timer timer = new Timer();
		KruskalMST mst = new KruskalMST(g);
		int wmst = mst.kruskal();
		timer.end();
		System.out.println(wmst);
	}
}
