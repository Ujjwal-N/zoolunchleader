package edu.sjsu.cs185c.here;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;;

public class Main {

    static String yourName;
    static String grpcHostPort;
    static String serverList;
    static String lunchPath;

    static ZooKeeper zk;
    static Semaphore mainThreadSem;

    @Command(name = "zoolunchleader", mixinStandardHelpOptions = true, description = "register attendance for class.")

    static class Cli implements Callable<Integer> {
        @Parameters(index = "0", description = "your_name")
        String yourName;

        @Parameters(index = "1", description = "grpcHostPort")
        String grpcHostPort;

        @Parameters(index = "2", description = "zookeeper_server_list")
        String serverList;

        @Parameters(index = "3", description = "lunch_path")
        String lunchPath;

        @Override
        public Integer call() throws Exception {
            mainThreadSem = new Semaphore(1);
            mainThreadSem.acquire();
            Main.yourName = this.yourName;
            Main.grpcHostPort = this.grpcHostPort;
            Main.serverList = this.serverList;
            Main.lunchPath = this.lunchPath;

            startZookeeper();
            mainThreadSem.acquire();
            return 0;
        }
    }

    public static void startZookeeper() {

        try {
            System.out.println(serverList);
            zk = new ZooKeeper(serverList, 10000, new ConnectedWatcher());

            try {
                System.out.println(zk.getChildren(lunchPath, null));
            } catch (KeeperException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            try {
                zk.exists(lunchPath + "/readyforlunch", (e) -> {
                    System.out.println("ready for lunch");
                    System.out.println(e);
                });

            } catch (Exception e) {
                System.out.println(e);
            }

        } catch (IOException e) {
            System.out.println("Zookeeper Server Node Supervisor not found");
            e.getStackTrace();
        }

    }

    static class ConnectedWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            // TODO Auto-generated method stub
            System.out.println("==============");
            System.out.println("Connected!");
            System.out.println("==============");
            System.out.println(event);
        }

    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Cli()).execute(args));
    }
}
