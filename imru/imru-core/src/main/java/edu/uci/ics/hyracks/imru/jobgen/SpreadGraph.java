package edu.uci.ics.hyracks.imru.jobgen;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Vector;

import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Distribute data out to every node in the cluster
 * in exactly log_2^{N} steps.
 * 
 * @author Rui Wang
 */
public class SpreadGraph {
    public static class Node implements Serializable {
        public int partitionInThisLevel;
        public String name;
        public Vector<Node> subNodes = new Vector<Node>();

        public Node(String name) {
            this.name = name;
        }

        public void print(int level) {
            for (int i = 0; i < level; i++)
                System.out.print("   ");
            System.out.println(name);
            for (Node n : subNodes)
                n.print(level + 1);
        }

        public int depth() {
            int depth = 1;
            for (Node n : subNodes) {
                int t = n.depth() + 1;
                if (t > depth)
                    depth = t;
            }
            return depth;
        }

        public void calculateLevels(Level[] levels, int level) {
            if (subNodes.size() == 0)
                return;
            levels[level + 1].nodes.addAll(subNodes);
            for (Node n : subNodes)
                n.calculateLevels(levels, level + 1);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static class Level implements Serializable {
        public Vector<Node> nodes = new Vector<Node>();
        public int level;

        public Level(int level) {
            this.level = level;
        }

        public String[] getLocationContraint() {
            String[] ss = new String[nodes.size()];
            for (int i = 0; i < ss.length; i++)
                ss[i] = nodes.get(i).name;
            return ss;
        }

        public Hashtable<Integer, int[]> getTargetPartitions() {
            Hashtable<Integer, int[]> hash = new Hashtable<Integer, int[]>();
            for (Node n : nodes) {
                int[] ps = new int[n.subNodes.size()];
                for (int i = 0; i < ps.length; i++)
                    ps[i] = n.subNodes.get(i).partitionInThisLevel;
                hash.put(n.partitionInThisLevel, ps);
            }
            return hash;
        }
    }

    public Node root;
    public int steps = 0;
    public Level[] levels;

    public SpreadGraph(String[] nodes, String startNode) {
        LinkedList<String> queue = new LinkedList<String>();
        HashSet<String> hash = new HashSet<String>();
        hash.add(startNode);
        for (String node : nodes) {
            if (!hash.contains(node)) {
                queue.add(node);
                hash.add(node);
            }
        }
        LinkedList<Node> acquriedNodes = new LinkedList<Node>();
        root = new Node(startNode);
        acquriedNodes.add(root);
        while (queue.size() > 0) {
            LinkedList<Node> newAcquriedNodes = new LinkedList<Node>();
            for (Node node : acquriedNodes) {
                if (queue.size() == 0)
                    break;

                Node n = new Node(queue.remove());
                node.subNodes.add(n);
                newAcquriedNodes.add(n);
            }
            acquriedNodes.addAll(newAcquriedNodes);
            steps++;
        }
        //        Rt.p(root.depth()+" "+ steps);
        levels = new Level[root.depth()];
        for (int i = 0; i < levels.length; i++)
            levels[i] = new Level(i);
        levels[0].nodes.add(root);
        root.calculateLevels(levels, 0);
        for (Level level : levels)
            for (int i = 0; i < level.nodes.size(); i++)
                level.nodes.get(i).partitionInThisLevel = i;
    }

    public void print() {
        root.print(0);
    }

    public static void main(String[] args) {
        for (int t = 1; t < 10; t++) {
            int size = 1 << t;
            String[] ss = new String[size];
            for (int i = 0; i < ss.length; i++)
                ss[i] = "" + i;
            SpreadGraph g = new SpreadGraph(ss, "0");
            //        g.print();
            Rt.p(size + " " + g.steps);
        }
        String[] ss = new String[15];
        for (int i = 0; i < ss.length; i++)
            ss[i] = "" + i;
        SpreadGraph g = new SpreadGraph(ss, "0");
        g.print();
        for (int i = 0; i < g.levels.length; i++) {
            Rt.p(i + " " + g.levels[i].nodes);
        }
    }
}
