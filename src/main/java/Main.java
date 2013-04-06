import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;


public class Main 
	{
		
		public static void main(String[] argv) 
			{
		
			Tap empTap = new FileTap(new TextLine(new Fields("empid")), "data/employee.txt");
			Tap salesTap = new FileTap(new TextLine(new Fields("custid")), "data/salesfact.txt");

			Tap resultsTap = new FileTap(new TextLine(new Fields("empid1", "custid1")), "output/results.txt");

			Pipe empPipe = new Pipe("emp");
			Pipe salesPipe = new Pipe("sales");

			Pipe join = new CoGroup(empPipe, new Fields("empid"), salesPipe, new Fields("custid"));

			FlowDef flowDef = FlowDef.flowDef()
				.setName("flow")
				.addSource(empPipe, empTap)
				.addSource(salesPipe, salesTap)
				.addTailSink(join, resultsTap);

			FlowConnector connector = new LocalFlowConnector();
			Flow flow = connector.connect(flowDef);
			flow.complete();

			}
	}
