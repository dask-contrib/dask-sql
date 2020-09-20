import { FC } from "react";

interface FooterTitleProps {}

const FooterTitle: FC<FooterTitleProps> = () => (
  <div className="flex-1 mb-6">
    <a
      className="no-underline hover:no-underline font-bold text-2xl lg:text-4xl"
      href="#"
    >
      dask-sql
    </a>
  </div>
);

export default FooterTitle;
