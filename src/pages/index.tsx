import { FC } from "react";
import Header from "../components/header";
import Footer from "../components/footer";
import Hero from "../components/hero";
import Features from "../components/features";
import ShowCase from "../components/showcase";
import Action from "../components/action/init";
import Topology from "../components/topology";

export interface HomePageProps { }

const HomePage: FC<HomePageProps> = () => {
  return (
    <div className="gradient leading-normal tracking-normal flex flex-col">
      <Header />
      <Hero />
      <ShowCase />
      <Features />
      <Topology />
      <Action />
      <Footer />
    </div>
  );
};

export default HomePage;
