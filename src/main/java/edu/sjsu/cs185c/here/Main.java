package edu.sjsu.cs185c.here;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;;

public class Main {

    // One time usage static references
    static Cli execObj;
    static byte[] grpcHostPort;
    static ZooKeeper zk;
    static Semaphore mainThreadSem;

    // Thread safe state class, to be modified by Watchers and FightForLeaderThread
    static class CurrentState {
        public static CurrentState singleton = new CurrentState();

        // flags
        private boolean _skipRequest = false; // set by grpc
        private boolean _readyForLunchExists = false; // set by readyforlunch watcher
        private boolean _iamleader = false;// set by fight for leader thread
        private boolean _lunchTimeExists = false; // set by lunchtime watcher

        // other objects
        private Stat _currentNodeStat; // set by ready for lunch watcher
        private long _sleepTime; // set by lunch time watcher
        private HashSet<String> _attendees = new HashSet<>(); // set by lunch time watcher

        // getters
        public synchronized boolean skipping() {
            return _skipRequest;
        }

        public synchronized boolean iamleader() {
            return _iamleader;
        }

        public synchronized boolean readyForLunch() {
            return _readyForLunchExists;
        }

        public synchronized boolean lunchTime() {
            return _lunchTimeExists;
        }

        public synchronized int getCurrentVersion() {
            if (_currentNodeStat == null) {
                return 0;
            }
            return _currentNodeStat.getVersion();
        }

        public synchronized long sleepTime() {
            return _sleepTime;
        }

        public synchronized HashSet<String> getAttendees() {
            return _attendees;
        }

        // setters
        public synchronized void skipNextLunch(boolean yes) {
            _skipRequest = yes;
        }

        public synchronized void setReadyForLunch(boolean created) {
            _readyForLunchExists = created;
        }

        public synchronized void amileader(boolean yes) {
            _iamleader = yes;
        }

        public synchronized void setLunchTime(boolean created) {
            _lunchTimeExists = created;
        }

        public synchronized void setCurrentStat(Stat newStat) {
            _currentNodeStat = newStat;
        }

        public synchronized void setSleepTime(long newSleepTime) {
            _sleepTime = newSleepTime;
        }

        public synchronized void addAttendee(String name) {
            _attendees.add(name);
        }

        public synchronized void removeAttendee(String name) {
            if (_attendees.contains(name)) {
                _attendees.remove(name);
            }
        }

        public synchronized void clearAttendees() {
            _attendees.clear();
        }

    }

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
            Main.grpcHostPort = this.grpcHostPort.getBytes();

            startZookeeper();
            mainThreadSem.acquire(); // main thread spinning
            return 0;
        }
    }

    public static void startZookeeper() { // starting point of program

        try {
            zk = new ZooKeeper(execObj.serverList, 10000, ConnectedWatcher.singleton);
        } catch (IOException e) {
            System.out.println("Zookeeper Server Node Supervisor not found");
            e.getStackTrace();
        }

    }

    static class ConnectedWatcher implements Watcher { // executes when connection is successful

        public static ConnectedWatcher singleton = new ConnectedWatcher();

        @Override
        public void process(WatchedEvent event) {

            System.out.println("==============");
            System.out.println("Connected!");
            System.out.println("==============");

            try {
                /*
                 * Useful call for debugging
                 * System.out.println(zk.getChildren(execObj.lunchPath, null));
                 */

                zk.create(execObj.lunchPath + "/employee/zk-" + execObj.yourName, grpcHostPort,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL); // registering in employee database
                zk.exists(execObj.lunchPath + "/readyforlunch", ReadyForLunchWatcher.singleton); // starting lunch stuff
            } catch (KeeperException | InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

        }
    }

    static class ReadyForLunchWatcher implements Watcher { // handles readyforlunch flag, sets watcher for lunchtime and
                                                           // resets watcher for itself

        public static ReadyForLunchWatcher singleton = new ReadyForLunchWatcher();

        @Override
        public void process(WatchedEvent event) {
            // making sure this method is always called with events relating to ready for
            // lunch
            try {
                zk.exists(execObj.lunchPath + "/readyforlunch", singleton);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (CurrentState.singleton.skipping()) { // if skipping, stop execution
                CurrentState.singleton.skipNextLunch(false);
                System.out.println("skipping :(");
                return;
            }

            if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                CurrentState.singleton.setReadyForLunch(true); // update flag

                try {
                    // saving stat for delete, joining current lunch
                    Stat myStat = null;
                    zk.create(execObj.lunchPath + "/zk-" + execObj.yourName, null,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL, myStat);
                    CurrentState.singleton.setCurrentStat(myStat);

                    System.out.println("==============");
                    System.out.println("Signed up for lunch!");
                    System.out.println("==============");

                    zk.exists(execObj.lunchPath + "/lunchtime", LunchTimeWatcher.singleton); // adding watcher for
                                                                                             // lunchtime
                    FightForLeader.singleton.start(); // attempting to become leader

                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                CurrentState.singleton.setReadyForLunch(false); // update flag
            }
        }
    }

    static class FightForLeader extends Thread { // handles am i leader flag, trys to become leader,sets watcher
                                                 // for leader

        static Thread singleton = new FightForLeader();

        @Override
        public void run() {
            try {
                Thread.sleep(CurrentState.singleton.sleepTime());

                Stat leaderStat = zk.exists(execObj.lunchPath + "/leader", LeaderWatcher.singleton); // check if leader
                                                                                                     // exists
                if (leaderStat != null) { // leader does exist
                    CurrentState.singleton.amileader(false); // update flag
                    return; // no need to try being leader
                }
                // can attempt being leader
                zk.create(execObj.lunchPath + "/leader", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                CurrentState.singleton.amileader(true);

                System.out.println("==============");
                System.out.println("Became leader!");
                System.out.println("==============");
            } catch (KeeperException | InterruptedException e) {
                CurrentState.singleton.amileader(false);
                e.printStackTrace();
            }

        }

    }

    static class LeaderWatcher implements Watcher { // constantly monitors leader, adds watches if it is leader, sets
                                                    // watcher for itself

        class AttendaceWatcher implements Watcher { // subwatcher to async add employees
            String emp = "";

            public AttendaceWatcher(String emp) {
                this.emp = emp;
            }

            @Override
            public void process(WatchedEvent event) {

                if (event.getType() != Watcher.Event.EventType.NodeDeleted) {
                    if (CurrentState.singleton.readyForLunch() && !CurrentState.singleton.lunchTime()
                            && !emp.equals("zk-" + execObj.yourName)) { // making sure the timing is right and not
                                                                        // adding self
                        CurrentState.singleton.addAttendee(emp); // can add many times, does not matter since it is a
                                                                 // hashset
                    }
                } else {
                    CurrentState.singleton.removeAttendee(emp); // the event was nodedeleted, so the list needs to be
                                                                // updated
                }

                try {
                    zk.exists(execObj.lunchPath + "/" + emp, this); // resetting watch since employee could go away
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        static LeaderWatcher singleton = new LeaderWatcher();

        @Override
        public void process(WatchedEvent event) {

            if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                if (CurrentState.singleton.iamleader()) {
                    try {
                        List<String> employees = zk.getChildren(execObj.lunchPath + "/employee", null); // getting all
                                                                                                        // connected
                                                                                                        // employees
                        // adding watches for all employees
                        for (String emp : employees) {
                            System.out.println("Adding watcher in " + execObj.lunchPath + "/" + emp);
                            zk.exists(execObj.lunchPath + "/" + emp, new AttendaceWatcher(emp));
                        }

                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                // re adding watch because leader might die
                try {
                    zk.exists(execObj.lunchPath + "/leader", singleton);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                if (CurrentState.singleton.readyForLunch()) {
                    FightForLeader.singleton.start(); // will re add this watch
                }
            }

        }

    }

    static class LunchTimeWatcher implements Watcher { // handles lunch time, sleep time flag, registers lunch, cleans
                                                       // up attendees list

        static LunchTimeWatcher singleton = new LunchTimeWatcher();

        @Override
        public void process(WatchedEvent event) {

            if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                CurrentState.singleton.setLunchTime(true); // updating flag

                if (CurrentState.singleton.iamleader()) {
                    try {

                        // writing data
                        HashSet<String> finalSet = CurrentState.singleton.getAttendees();
                        String dataString = execObj.yourName + "went to neveragain with \n"
                                + String.join("\n", finalSet);
                        System.out.println("==============");
                        System.out.println(dataString);
                        System.out.println("==============");
                        zk.create(execObj.lunchPath + "/pastlunches/lunch-" + execObj.yourName, dataString.getBytes(),
                                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT_SEQUENTIAL);

                        // updating sleep time
                        CurrentState.singleton.setSleepTime(finalSet.size() * 1000);

                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    CurrentState.singleton.setSleepTime(0); // updating sleep time
                }

            } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {

                CurrentState.singleton.setLunchTime(false); // updating flag
                try {
                    // cleaning up node
                    zk.delete(execObj.lunchPath + "/zk-" + execObj.yourName,
                            CurrentState.singleton.getCurrentVersion());
                    System.out.println("==============");
                    System.out.println("Lunch is over!");
                    System.out.println("==============");
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }

            }

        }

    }

    public static void main(String[] args) {
        execObj = new Cli();
        System.exit(new CommandLine(execObj).execute(args));
    }
}
