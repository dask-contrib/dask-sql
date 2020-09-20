import { FC } from "react";
import Card from "./card";

interface FeaturesProps {}

const Features: FC<FeaturesProps> = () => (
  <section className="bg-white py-12 ">
    <div className="container mx-auto flex flex-wrap items-start justify-between pb-12">
      <h2 className="w-full my-2 text-3xl font-black leading-tight text-center text-gray-800 lg:mt-8">
        Features
      </h2>
      <Card
        img="images/undraw_data_reports_706v.svg"
        header="Python and SQL"
        title="dask and dask-sql"
      >
        Query your data with both the <code>dask</code> API and normal SQL
        syntax in combination without the need for a database.
      </Card>
      <Card
        img="images/undraw_server_q2pb.svg"
        header="Infinite Scaling"
        title="Use the full power of your cluster"
      >
        You are writing normal SQL - but in the background your queries get
        distributed over your cluster and leverage the full computation power.
      </Card>
      <Card
        img="images/undraw_code_inspection_bdl7.svg"
        header="Your data - your queries"
        title="Mix SQL with custom functions"
      >
        Combine SQL functions with your self-written, python functions without
        any performance drawbacks or rewriting.
      </Card>
    </div>
  </section>
);

export default Features;
