import { FC } from "react";

interface HeaderProps {}

const Header: FC<HeaderProps> = () => (
  <nav className="w-full z-30 top-0">
    <div className="w-full container mx-auto flex flex-wrap items-center justify-between mt-0 py-2">
      <div className="pl-4 flex items-center">
        <a
          className="no-underline hover:no-underline font-bold text-2xl lg:text-4xl"
          href="#"
        >
          dask-sql
        </a>
      </div>
    </div>
  </nav>
);

export default Header;
