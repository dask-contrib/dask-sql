import { FC } from "react";

interface ShowCaseProps {}

const ShowCase: FC<ShowCaseProps> = () => (
  <section className="bg-white">
    <div className="container mx-auto">
      <div className="flex items-center md:-mt-40 w-4/5 mx-auto content-end">
        <div className="browser-mockup bg-gray-400 flex flex-1 m-6 md:px-0 md:m-12 w-1/2 rounded shadow-xl">
          <div className="bg-black w-full h-full overflow-x-auto">
            <p className="text-white font-mono text-base p-10">
              <span className="whitespace-no-wrap text-gray-500">
                # Load the data with dask
              </span>
              <br />
              <span className="whitespace-no-wrap">
                <span className="text-purple-500 font-bold">import</span>{" "}
                dask.datasets
              </span>
              <br />
              <span className="whitespace-no-wrap">
                df = dask.datasets.timeseries()
              </span>
              <br />
              <br />

              <span className="whitespace-no-wrap text-gray-500">
                # Register the data in dask-sql
              </span>
              <br />
              <span className="whitespace-no-wrap">
                <span className="text-purple-500 font-bold">from</span> dask_sql{" "}
                <span className="text-purple-500 font-bold">import</span>{" "}
                Context
              </span>
              <br />
              <span className="whitespace-no-wrap">c = Context()</span>
              <br />
              <span className="whitespace-no-wrap">
                c.register_dask_table(df,{" "}
                <span className="text-indigo-400">"timeseries"</span>)
              </span>
              <br />
              <br />

              <span className="whitespace-no-wrap text-gray-500">
                # Query the data utilizing your dask cluster with SQL
              </span>
              <br />
              <span className="whitespace-no-wrap">
                result = c.sql(
                <span className="text-indigo-400">
                  "SELECT name, SUM(x) FROM timeseries GROUP BY name"
                </span>
                ).compute()
              </span>
              {/* <span className="whitespace-no-wrap">
                from dask.datasets import timeseries
              </span>
              <br />
              <span className="whitespace-no-wrap">
                from dask_sql import Context
              </span>
              <br />
              <br />
              <span className="whitespace-no-wrap">
                timeseries, y = load_robot_execution_failures()
              </span>
              <br />
              <br />
              <span className="whitespace-no-wrap">
                <span>
                  features = extract_relevant_features(timeseries, y, column_id=
                </span>
                <span className="text-blue-600">"id"</span>, column_sort=
                <span className="text-blue-600">"time"</span>)
              </span>
              <br /> */}
            </p>
          </div>
        </div>
      </div>
    </div>
  </section>
);

export default ShowCase;
