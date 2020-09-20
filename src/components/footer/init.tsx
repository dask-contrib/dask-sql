import { FC } from "react";
import FooterTitle from "./footerTitle";
import FooterItem from "./footerItem";

interface FooterProps {}

const Footer: FC<FooterProps> = () => (
  <footer className="bg-white">
    <div className="container mx-auto mt-5 px-8">
      <div className="w-full flex flex-col md:flex-row py-6">
        <FooterTitle />

        <FooterItem
          title="GitHub"
          links={
            new Map([
              [
                "ReadMe",
                "https://github.com/nils-braun/dask-sql/blob/main/README.md",
              ],
              ["Code", "https://github.com/nils-braun/dask-sql/"],
              ["Contribute", "https://github.com/nils-braun/dask-sql/pulls"],
            ])
          }
        />

        <FooterItem
          title="Docs"
          links={
            new Map([
              ["Stable", "https://dask-sql.readthedocs.io/en/stable/"],
              ["Latest", "https://dask-sql.readthedocs.io/en/latest/"],
            ])
          }
        />

        <FooterItem
          title="Issues"
          links={
            new Map([
              ["List", "https://github.com/nils-braun/dask-sql/issues"],
              [
                "Create",
                "https://github.com/nils-braun/dask-sql/issues/new/choose",
              ],
            ])
          }
        />
      </div>
      <div className="w-full text-center text-gray-700 text-xs m-4">
        <p>
          Created by the{" "}
          <a className="underline" href="mailto:nilslennartbraun@gmail.com">
            dask-sql authors
          </a>
        </p>
        <p>
          This work is licensed under{" "}
          <a
            className="underline"
            href="https://creativecommons.org/licenses/by-sa/4.0/"
          >
            CC BY-SA 4.0
          </a>
          .
        </p>
      </div>
    </div>
  </footer>
);

export default Footer;
