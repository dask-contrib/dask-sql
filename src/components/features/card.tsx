import { FC } from "react";

export interface CardProps {
  title: string;
  header: string;
  img: string;
}

const Card: FC<CardProps> = (props) => (
  <div className="w-full md:w-1/3 p-6 flex flex-col flex-grow flex-shrink">
    <div className="flex-1 rounded-t rounded-b-none overflow-hidden shadow hover:shadow-lg">
      <img className="h-48 mx-auto mb-10" src={props.img} />
      <p className="w-full text-gray-600 text-xs md:text-sm px-6 mt-6">
        {props.title.toUpperCase()}
      </p>
      <div className="w-full mb-2 font-bold text-xl text-gray-800 px-6">
        {props.header}
      </div>
      <p className="text-gray-600 text-base px-6 mb-5">{props.children}</p>
    </div>
  </div>
);

export default Card;
