import { FC } from "react";

interface FooterItemProps {
  title: string;
  links: Map<string, string>;
}

const FooterItem: FC<FooterItemProps> = (props) => {
  let ulContent = [];

  for (const name of props.links.keys()) {
    ulContent.push(
      <li key={name} className="mt-2 inline-block mr-2 md:block md:mr-0">
        <a
          href={props.links.get(name)}
          className="no-underline hover:underline text-gray-800"
        >
          {name}
        </a>
      </li>
    );
  }

  return (
    <div className="flex-1">
      <p className="uppercase text-blue-800 md:mb-6">{props.title}</p>
      <ul className="list-reset mb-6">{ulContent.map((content) => content)}</ul>
    </div>
  );
};

export default FooterItem;
