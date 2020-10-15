import { FC, ReactNode } from "react";

interface TopologyTitleProps {
    children: ReactNode
}

const TopologyTitle: FC<TopologyTitleProps> = (props) => (
    <h1 className="text-lg text-center font-bold w-4/5 mx-auto">{props.children}</h1>
)

interface TopologyTextProps {
    children: ReactNode
}

const TopologyText: FC<TopologyTextProps> = (props) => (
    <div className="w-2/3 mx-auto mt-4 mb-4 italic text-sm text-center">
        {props.children}
    </div>
)

interface TopologyCardProps {
    children: ReactNode
    col: number
    row: number
    span: number
}

const TopologyCard: FC<TopologyCardProps> = (props) => (
    <div className={"my-auto bg-gray-700 rounded hover:shadow-2xl shadow-md p-4 lg:col-start-" + props.col + " lg:row-start-" + props.row + " lg:row-span-" + props.span}>
        {props.children}
    </div>
)


interface TopologyProps { }

const Topology: FC<TopologyProps> = () => (
    <section className="w-full bg-gray-200  bg-white">
        <div className="my-16 text-white p-6">
            <div className="w-4/5 grid mx-auto lg:grid-cols-4 lg:grid-rows-2 gap-10 lg:place-content-stretch">
                <TopologyCard col={1} row={1} span={2}>
                    <TopologyTitle>Computing Infrastructure</TopologyTitle>
                    <TopologyText>
                        As <code>dask-sql</code> utilizes dask,
                        you can use the large variety of possible
                        computing infrastructures that dask supports:
                        cloud providers, YARN, k8s, batch systems, ...
                    </TopologyText>
                </TopologyCard>
                <TopologyCard col={2} row={1} span={2}>
                    <TopologyTitle>dask Cluster</TopologyTitle>
                    <TopologyText>
                        Connect to your infrastructure
                        and deploy a dask scheduler to control
                        your computation.
                        There are <a className="underline" href="https://docs.dask.org/en/latest/setup.html">many</a> ways to do so.
                        By default, a local cluster is spawned for you.
                    </TopologyText>
                </TopologyCard>
                <TopologyCard col={3} row={1} span={1}>
                    <TopologyTitle>dask-sql Library</TopologyTitle>
                    <TopologyText>
                        <code>dask-sql</code> will connect to your
                        dask cluster (and the computing infrastructure)
                        and will translate your SQL queries into
                        dask API calls, which are executed on your cluster.
                    </TopologyText>
                </TopologyCard>
                <TopologyCard col={3} row={2} span={1}>
                    <TopologyTitle>dask-sql SQL server</TopologyTitle>
                    <TopologyText>
                        It is also possible to deploy a <code>dask-sql</code>
                        service, e.g. with a docker image,
                        which lets you send SQL queries via a presto-compatible
                        REST interface.
                    </TopologyText>
                </TopologyCard>
                <TopologyCard col={4} row={1} span={1}>
                    <TopologyTitle>Notebooks and Code</TopologyTitle>
                    <TopologyText>
                        You can import <code>dask-sql</code>
                        into your own scripts and mix SQL code
                        with your normal dataframe operations.
                        It works particularly well with the
                        interactive notebook format.
                    </TopologyText>
                </TopologyCard>
                <TopologyCard col={4} row={2} span={1}>
                    <TopologyTitle>External Applications, e.g. BI Tools</TopologyTitle>
                    <TopologyText>
                        Using a well-known protocol and the standardized SQL
                        language, <code>dask-sql</code> helps you to query
                        your data from wherever you want.
                    </TopologyText>
                </TopologyCard>
            </div>
        </div>
    </section>
)

export default Topology;