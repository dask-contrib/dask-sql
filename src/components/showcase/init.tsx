import { FC } from "react";

import { CodeLine, Keyword, String, Comment } from "./code"

interface ShowCaseProps { }

const ShowCase: FC<ShowCaseProps> = () => (
  <section className="bg-white">
    <div className="container mx-auto">
      <div className="flex items-center md:-mt-40 w-4/5 mx-auto content-end">
        <div className="browser-mockup bg-gray-400 flex flex-1 m-6 md:px-0 md:m-12 w-1/2 rounded shadow-xl">
          <div className="bg-black w-full h-full overflow-x-auto">
            <div className="text-white font-mono text-base p-10">
              <CodeLine indent={0}>
                <Keyword>if</Keyword> __name__ == <String>"__main__"</String>:
              </CodeLine>
              <CodeLine indent={4}><Comment>
                # Create a dask cluster
              </Comment></CodeLine>
              <CodeLine indent={4}>
                <Keyword>from</Keyword> dask.distributed <Keyword>import</Keyword> Client
              </CodeLine>
              <CodeLine indent={4}>
                client = Client()
              </CodeLine>
              <br />
              <CodeLine indent={4}><Comment>
                # Load the data with dask
              </Comment></CodeLine>
              <CodeLine indent={4}>
                <Keyword>import</Keyword> dask.datasets
              </CodeLine>
              <CodeLine indent={4}>
                df = dask.datasets.timeseries()
              </CodeLine>
              <br />
              <CodeLine indent={4}><Comment>
                # Register the data in a dask-sql context
              </Comment></CodeLine>
              <CodeLine indent={4}>
                <Keyword>from</Keyword> dask_sql <Keyword>import</Keyword> Context
              </CodeLine>
              <CodeLine indent={4}>c = Context()</CodeLine>
              <CodeLine indent={4}>
                c.register_dask_table(df, <String>"timeseries"</String>)
              </CodeLine>
              <br />
              <CodeLine indent={4}><Comment>
                # Query the data utilizing your dask cluster with standard SQL
              </Comment></CodeLine>
              <CodeLine indent={4}>
                result = c.sql(
                <String>
                  "SELECT name, SUM(x) FROM timeseries GROUP BY name"
                  </String>
                ).compute()
              </CodeLine>
              <CodeLine indent={4}>
                print(result)
              </CodeLine>
            </div>
          </div>
        </div>
      </div>
    </div>
  </section >
);

export default ShowCase;
