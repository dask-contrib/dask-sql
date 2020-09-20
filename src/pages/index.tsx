import { FC } from "react";
import Header from "../components/header/init";
import Footer from "../components/footer/init";
import Hero from "../components/hero/init";
import Features from "../components/features/init";
import ShowCase from "../components/showcase/init";
import Action from "../components/action/init";

export interface HomePageProps {}

const HomePage: FC<HomePageProps> = () => {
  return (
    <div className="gradient leading-normal tracking-normal flex flex-col">
      <Header />
      <Hero />
      <ShowCase />
      <Features />
      <Action />
      <Footer />
    </div>
  );
};

export default HomePage;
